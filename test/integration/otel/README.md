# OpenTelemetry Integration Tests

This module contains integration tests for validating distributed tracing attributes in Thanos using an embedded OTLP server.

## Overview

Unlike the e2e tests which use Docker containers, these integration tests:
- Run as a **separate Go module** with its own dependencies
- **Embed a lightweight OTLP gRPC server** using protobuf definitions
- **Build Thanos binaries** from source for each test run
- Run **Prometheus**, **Thanos Sidecar**, and **Thanos Query** as subprocesses
- Configure all components to send traces to the embedded OTLP server
- Validate trace span attributes programmatically

## Architecture

```
┌──────────────┐
│  Test Runner │
│   (Go test)  │
└───────┬──────┘
        │
        ├─ Build Thanos Binary
        │
        ├─ Start Embedded OTLP Server
        │       └─ Lightweight gRPC server (localhost:14317)
        │
        ├─ Start Prometheus (subprocess)
        │       └─ Scrapes itself
        │
        ├─ Start Thanos Sidecar (subprocess)
        │       └─ OTLP traces → localhost:14317
        │
        ├─ Start Thanos Query (subprocess)
        │       └─ OTLP traces → localhost:14317
        │
        ├─ Execute Test Queries
        │       └─ HTTP → Query → Sidecar
        │
        └─ Validate Trace Attributes
                └─ Check OTLP server's collected spans
```

## Prerequisites

- **Go 1.23 or later**
- **Prometheus binary** in PATH (for test setup)
- **Docker** (only for optional manual testing)

## Running the Tests

### Quick Start

```bash
cd test/integration/otel

# Download dependencies
go mod download

# Run all tests
go test -v -timeout 10m ./...

# Run specific test
go test -v -timeout 10m -run TestTraceAttributesWithEmbeddedCollector
```

### With Makefile

```bash
# Install dependencies
make deps

# Run tests
make test

# Run tests with verbose output
make test-verbose

# Clean build artifacts
make clean
```

## Validated Trace Attributes

### Query gRPC Handlers

**Operation**: `/thanos.Query/Query` (instant query)
**Operation**: `/thanos.Query/QueryRange` (range query)

- `query.expr` (string) - The PromQL query expression
- `result.series` (int64) - Number of series returned
- `result.samples` (int64) - Number of samples returned
- `result.estimated_bytes` (int64) - Estimated in-memory size
- `result.wire_bytes` (int64) - gRPC wire protocol size

### Series gRPC Handlers

**Operation**: `proxy.series` or `/thanos.Store/Series`

- `series.selector` (string) - Label matchers for the request
- `result.series` (int64) - Number of series returned
- `result.samples` (int64) - Number of chunks/samples returned
- `result.estimated_bytes` (int64) - Estimated in-memory size
- `result.wire_bytes` (int64) - gRPC wire protocol size

## Test Implementation Details

### Embedded OTLP Collector

The test uses the OpenTelemetry Collector SDK to run an OTLP receiver in-process:

```go
// Create receiver configuration
cfg := &otlpreceiver.Config{
    GRPC: &configgrpc.ServerConfig{
        NetAddr: configgrpc.NetAddr{
            Endpoint: "localhost:14317",
        },
    },
}

// Start receiver with custom consumer that stores traces
receiver, _ := factory.CreateTraces(ctx, set, cfg, traceConsumer)
receiver.Start(ctx, host)
```

### Binary Building

Each test run builds fresh Thanos binaries:

```bash
go build -o /tmp/test-xxx/thanos ./cmd/thanos
```

This ensures tests run against the latest code changes.

### Trace Storage

The embedded collector stores all received traces in memory:

```go
type embeddedCollector struct {
    traces []ptrace.Traces  // All received trace data
}

func (c *embeddedCollector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
    c.traces = append(c.traces, td)
    return nil
}
```

### Trace Validation

Tests iterate through all spans to find specific attributes:

```go
for _, trace := range collector.GetTraces() {
    for span := range trace.AllSpans() {
        if span.Name() == "/thanos.Query/Query" {
            attrs := span.Attributes()
            if val, ok := attrs.Get("query.expr"); ok {
                // Validate attribute value
            }
        }
    }
}
```

## Directory Structure

```
test/integration/otel/
├── go.mod                 # Separate Go module
├── go.sum                 # Dependency checksums
├── trace_test.go          # Main integration test
├── README.md              # This file
├── Makefile               # Build automation
└── .gitignore             # Git ignore rules
```

## Troubleshooting

### Build Failures

**Problem**: Failed to build Thanos binary
```
Solution:
1. Ensure you're in the repository root
2. Check that all dependencies are available: go mod download
3. Try building manually: go build ./cmd/thanos
```

### Port Conflicts

**Problem**: Address already in use
```
Solution:
1. Check for conflicting processes: netstat -tuln | grep -E '9090|14317|19090'
2. Kill conflicting processes
3. Tests use random ports where possible
```

### Prometheus Not Found

**Problem**: exec: "prometheus": executable file not found in $PATH
```
Solution:
1. Install Prometheus: https://prometheus.io/download/
2. Or use Docker: docker run -d --name prom prom/prometheus
3. Or specify path: export PATH=$PATH:/path/to/prometheus
```

### No Traces Collected

**Problem**: Test fails with "no traces collected"
```
Solution:
1. Check OTLP collector logs in test output
2. Verify Thanos components started successfully
3. Increase wait time: time.Sleep(10 * time.Second)
4. Check trace configuration files are created correctly
```

### Test Timeout

**Problem**: Test exceeds timeout
```
Solution:
1. Increase timeout: go test -v -timeout 15m
2. Check system resources (CPU, memory)
3. Review subprocess logs for hangs
```

## Development

### Adding New Trace Attributes

1. **Add attribute to Thanos code**:
   ```go
   // In pkg/api/query/grpc.go or pkg/store/*.go
   if span := opentracing.SpanFromContext(ctx); span != nil {
       span.SetTag("my.new.attribute", value)
   }
   ```

2. **Add validation to test**:
   ```go
   // In trace_test.go
   if val, ok := attrs.Get("my.new.attribute"); ok {
       require.Equal(t, expectedValue, val.AsString())
       foundMyAttribute = true
   }
   require.True(t, foundMyAttribute, "my.new.attribute not found")
   ```

3. **Update documentation**:
   - Add to "Validated Trace Attributes" section
   - Update any relevant examples

### Running Tests in CI

Example GitHub Actions workflow:

```yaml
name: OTEL Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Install Prometheus
        run: |
          wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
          tar xzf prometheus-2.45.0.linux-amd64.tar.gz
          sudo cp prometheus-2.45.0.linux-amd64/prometheus /usr/local/bin/

      - name: Run Integration Tests
        working-directory: test/integration/otel
        run: |
          go mod download
          go test -v -timeout 15m ./...
```

### Debugging Tests

Enable verbose logging:

```bash
# Set log level for Thanos components
go test -v -run TestTraceAttributesWithEmbeddedCollector

# Check subprocess output in test logs
# Stdout/Stderr from Prometheus, Sidecar, and Query are visible

# Add debug logging to test
t.Logf("Trace count: %d", len(collector.GetTraces()))
for _, trace := range collector.GetTraces() {
    t.Logf("Trace: %+v", trace)
}
```

### Manual Testing

To inspect the embedded collector's behavior:

```go
// In trace_test.go, add after executeTestQueries():
for i, trace := range collector.GetTraces() {
    t.Logf("=== Trace %d ===", i)
    for j := 0; j < trace.ResourceSpans().Len(); j++ {
        rs := trace.ResourceSpans().At(j)
        t.Logf("Resource: %v", rs.Resource().Attributes().AsRaw())
        // ... inspect spans
    }
}
```

## Comparison with E2E Tests

| Feature | E2E Tests (test/e2e) | OTEL Tests (test/integration/otel) |
|---------|---------------------|-------------------------------------|
| **Infrastructure** | Docker containers | Embedded collector + subprocesses |
| **Dependencies** | Docker, e2e framework | Go SDK only |
| **Trace Backend** | Jaeger container | In-memory OTLP receiver |
| **Speed** | Slower (container startup) | Faster (in-process) |
| **Isolation** | Full isolation | Process-level isolation |
| **Debugging** | External Jaeger UI | Programmatic inspection |
| **CI Complexity** | Requires Docker | Go-only |
| **Use Case** | End-to-end validation | Unit-like integration tests |

## References

- [OpenTelemetry Collector Documentation](https://opentelemetry.io/docs/collector/)
- [OpenTelemetry Go SDK](https://pkg.go.dev/go.opentelemetry.io/otel)
- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [Thanos Tracing Documentation](https://thanos.io/tip/thanos/tracing.md/)
