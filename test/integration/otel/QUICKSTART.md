# Quick Start Guide

## Overview

This integration test validates that Thanos components correctly emit distributed tracing span attributes when configured with OpenTelemetry.

## Key Features

✅ **Embedded OTLP Server** - Runs a lightweight OTLP gRPC server in-process to collect traces
✅ **Automatic Binary Building** - Builds Thanos from source for each test run
✅ **Full Stack Testing** - Tests Prometheus → Sidecar → Query chain
✅ **Programmatic Validation** - Validates trace attributes directly in Go code
✅ **No External Dependencies** - Uses minimal OTLP protobuf definitions without full collector framework

## Running the Tests

### Prerequisites

Install Prometheus (required for test setup):

```bash
# macOS
brew install prometheus

# Linux
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xzf prometheus-2.45.0.linux-amd64.tar.gz
sudo mv prometheus-2.45.0.linux-amd64/prometheus /usr/local/bin/
```

### Run the Test

```bash
cd test/integration/otel

# Run all tests
make test

# Or use go test directly
go test -v -timeout 10m ./...
```

### What the Test Does

1. **Starts an embedded OTLP collector** on localhost:14317 (gRPC) and localhost:14318 (HTTP)
2. **Builds Thanos binary** from the repository source code
3. **Starts Prometheus** and configures it to scrape itself
4. **Starts Thanos Sidecar** configured to send OTLP traces to the collector
5. **Starts Thanos Query** configured to send OTLP traces to the collector
6. **Executes test queries** (instant and range queries)
7. **Validates trace attributes** are present in collected spans

### Expected Output

```
=== RUN   TestTraceAttributesWithEmbeddedCollector
    trace_test.go:37: Building Thanos binaries...
    trace_test.go:42: Starting embedded OTLP collector...
    trace_test.go:153: OTLP collector started on localhost:14317 (gRPC) and localhost:14318 (HTTP)
    trace_test.go:48: Starting Prometheus...
    trace_test.go:52: Starting Thanos Sidecar with OTLP tracing...
    trace_test.go:58: Starting Thanos Query with OTLP tracing...
    trace_test.go:66: Executing test queries...
    trace_test.go:72: Waiting for traces to be collected...
=== RUN   TestTraceAttributesWithEmbeddedCollector/QueryTraceAttributes
    trace_test.go:433: Found query.expr: up
    trace_test.go:439: Found result.series: 1
    trace_test.go:445: Found result.samples: 1
    trace_test.go:451: Found result.estimated_bytes: 48
    trace_test.go:457: Found result.wire_bytes: 156
=== RUN   TestTraceAttributesWithEmbeddedCollector/SeriesTraceAttributes
    trace_test.go:488: Found series.selector: {__name__="up"}
    trace_test.go:494: Found result.series: 1
    trace_test.go:500: Found result.samples: 1
    trace_test.go:506: Found result.estimated_bytes: 240
    trace_test.go:512: Found result.wire_bytes: 512
--- PASS: TestTraceAttributesWithEmbeddedCollector (45.32s)
    --- PASS: TestTraceAttributesWithEmbeddedCollector/QueryTraceAttributes (0.01s)
    --- PASS: TestTraceAttributesWithEmbeddedCollector/SeriesTraceAttributes (0.01s)
PASS
ok  	github.com/thanos-io/thanos/test/integration/otel	45.321s
```

## Validated Attributes

### Query Operations (`/thanos.Query/Query`, `/thanos.Query/QueryRange`)

| Attribute | Type | Description |
|-----------|------|-------------|
| `query.expr` | string | The PromQL query expression |
| `result.series` | int64 | Number of series returned |
| `result.samples` | int64 | Number of samples returned |
| `result.estimated_bytes` | int64 | Estimated in-memory size |
| `result.wire_bytes` | int64 | gRPC wire protocol size |

### Series Operations (`proxy.series`, `/thanos.Store/Series`)

| Attribute | Type | Description |
|-----------|------|-------------|
| `series.selector` | string | Label matchers for the request |
| `result.series` | int64 | Number of series returned |
| `result.samples` | int64 | Number of chunks/samples |
| `result.estimated_bytes` | int64 | Estimated in-memory size |
| `result.wire_bytes` | int64 | gRPC wire protocol size |

## Troubleshooting

### Test Fails to Build Thanos

**Error**: `failed to build Thanos binary`

**Solution**: Ensure you're in the correct directory structure:
```bash
cd /path/to/thanos/test/integration/otel
ls ../../../cmd/thanos  # Should exist
```

### Prometheus Not Found

**Error**: `exec: "prometheus": executable file not found in $PATH`

**Solution**: Install Prometheus or add it to PATH:
```bash
export PATH=$PATH:/path/to/prometheus
```

### Port Already in Use

**Error**: `bind: address already in use`

**Solution**: Kill processes using the ports:
```bash
# Check what's using the ports
lsof -i :9090 -i :14317 -i :19090 -i :19190

# Kill the processes
kill $(lsof -t -i :9090 -i :14317 -i :19090 -i :19190)
```

### No Traces Collected

**Error**: `no traces collected`

**Solution**:
1. Check that all components started successfully (look for error messages in test output)
2. Increase wait time after executing queries
3. Verify OTLP collector logs show received traces

## Architecture

```
┌─────────────────────────────────────────┐
│         Test Process (Go)               │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  Embedded OTLP Collector         │  │
│  │  - gRPC: localhost:14317         │  │
│  │  - HTTP: localhost:14318         │  │
│  │  - Stores traces in memory       │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
                   ▲
                   │ OTLP Traces
                   │
        ┌──────────┴──────────┐
        │                     │
┌───────▼──────┐    ┌─────────▼──────┐
│   Sidecar    │    │     Query      │
│   :19090     │◄───┤     :19192     │
└──────▲───────┘    └────────────────┘
       │                     ▲
       │                     │ HTTP Query
       │                     │
┌──────▼───────┐    ┌────────▼────────┐
│  Prometheus  │    │      Test       │
│    :9090     │    │   (HTTP Client) │
└──────────────┘    └─────────────────┘
```

## Next Steps

- Review the full [README.md](README.md) for detailed documentation
- Explore the [Makefile](Makefile) for additional commands
- Check [trace_test.go](trace_test.go) to see the implementation
- Add new test cases for additional trace attributes
