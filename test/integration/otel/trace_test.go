// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otel_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.uber.org/zap"
)

// TestTraceAttributesWithEmbeddedCollector validates trace attributes using an embedded OTLP collector
func TestTraceAttributesWithEmbeddedCollector(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create test directories
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	// Build Thanos binaries
	t.Log("Building Thanos binaries...")
	thanosBinary := buildThanosBinary(t, tmpDir)

	// Start embedded OTLP collector
	t.Log("Starting embedded OTLP collector...")
	collector := newEmbeddedCollector(t)
	collectorAddr := collector.Start(ctx, t)
	defer collector.Stop()

	// Start Prometheus
	t.Log("Starting Prometheus...")
	promConfig := createPrometheusConfig(t, tmpDir)
	promPort := 9090
	promCmd := startPrometheus(t, ctx, tmpDir, promConfig, promPort)
	defer promCmd.Process.Kill()
	waitForPrometheus(t, ctx, promPort)

	// Start Thanos Sidecar with OTLP tracing
	t.Log("Starting Thanos Sidecar with OTLP tracing...")
	sidecarGRPCPort := 19090
	sidecarHTTPPort := 19091
	sidecarCmd := startThanosSidecar(t, ctx, thanosBinary, promPort, sidecarGRPCPort, sidecarHTTPPort, collectorAddr, tmpDir)
	defer sidecarCmd.Process.Kill()
	waitForThanos(t, ctx, sidecarHTTPPort)

	// Start Thanos Query with OTLP tracing
	t.Log("Starting Thanos Query with OTLP tracing...")
	queryGRPCPort := 19190
	queryHTTPPort := 19192
	queryCmd := startThanosQuery(t, ctx, thanosBinary, sidecarGRPCPort, queryGRPCPort, queryHTTPPort, collectorAddr, tmpDir)
	defer queryCmd.Process.Kill()
	waitForThanos(t, ctx, queryHTTPPort)

	// Execute test queries to generate traces
	t.Log("Executing test queries...")
	executeTestQueries(t, ctx, queryHTTPPort)

	// Wait for traces to be collected
	t.Log("Waiting for traces to be collected...")
	time.Sleep(5 * time.Second)

	// Validate trace attributes
	t.Run("QueryTraceAttributes", func(t *testing.T) {
		validateQueryTraceAttributes(t, collector)
	})

	t.Run("SeriesTraceAttributes", func(t *testing.T) {
		validateSeriesTraceAttributes(t, collector)
	})
}

// embeddedCollector runs an OTLP receiver and stores received traces
type embeddedCollector struct {
	mu         sync.RWMutex
	traces     []ptrace.Traces
	receiver   receiver.Traces
	logger     *zap.Logger
	grpcServer *http.Server
	httpServer *http.Server
}

func newEmbeddedCollector(t *testing.T) *embeddedCollector {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	return &embeddedCollector{
		traces: make([]ptrace.Traces, 0),
		logger: logger,
	}
}

func (c *embeddedCollector) Start(ctx context.Context, t *testing.T) string {
	// Create a consumer that stores traces
	traceConsumer := &traceStorageConsumer{collector: c}

	// Configure OTLP receiver
	factory := otlpreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*otlpreceiver.Config)

	// Configure gRPC endpoint
	cfg.Protocols.GRPC = &configgrpc.ServerConfig{}
	cfg.Protocols.GRPC.NetAddr.Endpoint = "localhost:14317"

	// Configure HTTP endpoint
	cfg.Protocols.HTTP = &otlpreceiver.HTTPConfig{
		ServerConfig: &confighttp.ServerConfig{
			Endpoint: "localhost:14318",
		},
	}

	// Create receiver
	set := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: c.logger,
		},
	}

	var err error
	c.receiver, err = factory.CreateTraces(ctx, set, cfg, traceConsumer)
	require.NoError(t, err)

	// Start receiver
	err = c.receiver.Start(ctx, componentHost{})
	require.NoError(t, err)

	t.Log("OTLP collector started on localhost:14317 (gRPC) and localhost:14318 (HTTP)")
	return "localhost:14317"
}

func (c *embeddedCollector) Stop() {
	if c.receiver != nil {
		c.receiver.Shutdown(context.Background())
	}
}

func (c *embeddedCollector) GetTraces() []ptrace.Traces {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]ptrace.Traces{}, c.traces...)
}

func (c *embeddedCollector) addTrace(trace ptrace.Traces) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, trace)
}

// traceStorageConsumer implements consumer.Traces
type traceStorageConsumer struct {
	collector *embeddedCollector
}

func (t *traceStorageConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (t *traceStorageConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	t.collector.addTrace(td)
	t.collector.logger.Info("Received trace",
		zap.Int("resource_spans", td.ResourceSpans().Len()),
		zap.Int("total_spans", td.SpanCount()),
	)
	return nil
}

// componentHost implements component.Host (minimal implementation)
type componentHost struct{}

func (componentHost) ReportFatalError(err error)                          {}
func (componentHost) GetFactory(component.Kind, component.Type) component.Factory { return nil }
func (componentHost) GetExtensions() map[component.ID]component.Component { return nil }
func (componentHost) GetExporters() map[component.ID]component.Component  { return nil }

// Helper functions

func buildThanosBinary(t *testing.T, tmpDir string) string {
	t.Helper()

	// Get repository root (3 levels up from test/integration/otel)
	repoRoot, err := filepath.Abs("../../..")
	require.NoError(t, err)

	binaryPath := filepath.Join(tmpDir, "thanos")

	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/thanos")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Run(), "failed to build Thanos binary")

	return binaryPath
}

func createPrometheusConfig(t *testing.T, tmpDir string) string {
	t.Helper()

	config := `global:
  scrape_interval: 5s
  external_labels:
    prometheus: prom1
    replica: "0"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
`

	configPath := filepath.Join(tmpDir, "prometheus.yml")
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0644))

	return configPath
}

func startPrometheus(t *testing.T, ctx context.Context, tmpDir, configPath string, port int) *exec.Cmd {
	t.Helper()

	dataDir := filepath.Join(tmpDir, "prometheus-data")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	cmd := exec.CommandContext(ctx, "prometheus",
		"--config.file="+configPath,
		"--storage.tsdb.path="+dataDir,
		fmt.Sprintf("--web.listen-address=:%d", port),
		"--storage.tsdb.min-block-duration=2h",
		"--storage.tsdb.max-block-duration=2h",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Start())

	return cmd
}

func createTracingConfig(collectorAddr string) string {
	return fmt.Sprintf(`type: OTLP
config:
  client_type: grpc
  endpoint: %s
  insecure: true
  compression: gzip
`, collectorAddr)
}

func startThanosSidecar(t *testing.T, ctx context.Context, binary string, promPort, grpcPort, httpPort int, collectorAddr, tmpDir string) *exec.Cmd {
	t.Helper()

	tracingConfigPath := filepath.Join(tmpDir, "sidecar-tracing.yml")
	require.NoError(t, os.WriteFile(tracingConfigPath, []byte(createTracingConfig(collectorAddr)), 0644))

	cmd := exec.CommandContext(ctx, binary, "sidecar",
		fmt.Sprintf("--prometheus.url=http://localhost:%d", promPort),
		fmt.Sprintf("--grpc-address=0.0.0.0:%d", grpcPort),
		fmt.Sprintf("--http-address=0.0.0.0:%d", httpPort),
		"--tsdb.path="+filepath.Join(tmpDir, "prometheus-data"),
		"--tracing.config-file="+tracingConfigPath,
		"--log.level=debug",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Start())

	return cmd
}

func startThanosQuery(t *testing.T, ctx context.Context, binary string, sidecarGRPCPort, grpcPort, httpPort int, collectorAddr, tmpDir string) *exec.Cmd {
	t.Helper()

	tracingConfigPath := filepath.Join(tmpDir, "query-tracing.yml")
	require.NoError(t, os.WriteFile(tracingConfigPath, []byte(createTracingConfig(collectorAddr)), 0644))

	cmd := exec.CommandContext(ctx, binary, "query",
		fmt.Sprintf("--grpc-address=0.0.0.0:%d", grpcPort),
		fmt.Sprintf("--http-address=0.0.0.0:%d", httpPort),
		fmt.Sprintf("--store=localhost:%d", sidecarGRPCPort),
		"--tracing.config-file="+tracingConfigPath,
		"--log.level=debug",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Start())

	return cmd
}

func waitForPrometheus(t *testing.T, ctx context.Context, port int) {
	t.Helper()

	url := fmt.Sprintf("http://localhost:%d/-/ready", port)
	waitForEndpoint(t, ctx, url, 30*time.Second)
}

func waitForThanos(t *testing.T, ctx context.Context, port int) {
	t.Helper()

	url := fmt.Sprintf("http://localhost:%d/-/ready", port)
	waitForEndpoint(t, ctx, url, 30*time.Second)
}

func waitForEndpoint(t *testing.T, ctx context.Context, url string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("endpoint %s did not become ready within %v", url, timeout)
}

func executeTestQueries(t *testing.T, ctx context.Context, queryPort int) {
	t.Helper()

	baseURL := fmt.Sprintf("http://localhost:%d", queryPort)

	// Instant query
	instantURL := fmt.Sprintf("%s/api/v1/query?query=up", baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", instantURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("Instant query response: %s", string(body))

	// Range query
	now := time.Now()
	start := now.Add(-5 * time.Minute).Unix()
	end := now.Unix()
	rangeURL := fmt.Sprintf("%s/api/v1/query_range?query=up&start=%d&end=%d&step=15", baseURL, start, end)

	req, err = http.NewRequestWithContext(ctx, "GET", rangeURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("Range query response: %s", string(body))
}

func validateQueryTraceAttributes(t *testing.T, collector *embeddedCollector) {
	t.Helper()

	traces := collector.GetTraces()
	require.NotEmpty(t, traces, "no traces collected")

	foundQueryExpr := false
	foundResultSeries := false
	foundResultSamples := false
	foundEstimatedBytes := false
	foundWireBytes := false

	for _, trace := range traces {
		for i := 0; i < trace.ResourceSpans().Len(); i++ {
			rs := trace.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				for k := 0; k < ss.Spans().Len(); k++ {
					span := ss.Spans().At(k)
					spanName := span.Name()

					// Look for Query or QueryRange spans
					if spanName != "/thanos.Query/Query" && spanName != "/thanos.Query/QueryRange" {
						continue
					}

					attrs := span.Attributes()

					// Check for query.expr
					if val, ok := attrs.Get("query.expr"); ok {
						t.Logf("Found query.expr: %v", val.AsString())
						foundQueryExpr = true
					}

					// Check for result.series
					if val, ok := attrs.Get("result.series"); ok {
						t.Logf("Found result.series: %v", val.Int())
						foundResultSeries = true
					}

					// Check for result.samples
					if val, ok := attrs.Get("result.samples"); ok {
						t.Logf("Found result.samples: %v", val.Int())
						foundResultSamples = true
					}

					// Check for result.estimated_bytes
					if val, ok := attrs.Get("result.estimated_bytes"); ok {
						t.Logf("Found result.estimated_bytes: %v", val.Int())
						foundEstimatedBytes = true
					}

					// Check for result.wire_bytes
					if val, ok := attrs.Get("result.wire_bytes"); ok {
						t.Logf("Found result.wire_bytes: %v", val.Int())
						foundWireBytes = true
					}
				}
			}
		}
	}

	require.True(t, foundQueryExpr, "query.expr attribute not found in traces")
	require.True(t, foundResultSeries, "result.series attribute not found in traces")
	require.True(t, foundResultSamples, "result.samples attribute not found in traces")
	require.True(t, foundEstimatedBytes, "result.estimated_bytes attribute not found in traces")
	require.True(t, foundWireBytes, "result.wire_bytes attribute not found in traces")
}

func validateSeriesTraceAttributes(t *testing.T, collector *embeddedCollector) {
	t.Helper()

	traces := collector.GetTraces()
	require.NotEmpty(t, traces, "no traces collected")

	foundSeriesSelector := false
	foundResultSeries := false
	foundResultSamples := false
	foundEstimatedBytes := false
	foundWireBytes := false

	for _, trace := range traces {
		for i := 0; i < trace.ResourceSpans().Len(); i++ {
			rs := trace.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				for k := 0; k < ss.Spans().Len(); k++ {
					span := ss.Spans().At(k)
					spanName := span.Name()

					// Look for Series spans
					if spanName != "proxy.series" && spanName != "/thanos.Store/Series" {
						continue
					}

					attrs := span.Attributes()

					// Check for series.selector
					if val, ok := attrs.Get("series.selector"); ok {
						t.Logf("Found series.selector: %v", val.AsString())
						foundSeriesSelector = true
					}

					// Check for result.series
					if val, ok := attrs.Get("result.series"); ok {
						t.Logf("Found result.series: %v", val.Int())
						foundResultSeries = true
					}

					// Check for result.samples
					if val, ok := attrs.Get("result.samples"); ok {
						t.Logf("Found result.samples: %v", val.Int())
						foundResultSamples = true
					}

					// Check for result.estimated_bytes
					if val, ok := attrs.Get("result.estimated_bytes"); ok {
						t.Logf("Found result.estimated_bytes: %v", val.Int())
						foundEstimatedBytes = true
					}

					// Check for result.wire_bytes
					if val, ok := attrs.Get("result.wire_bytes"); ok {
						t.Logf("Found result.wire_bytes: %v", val.Int())
						foundWireBytes = true
					}
				}
			}
		}
	}

	require.True(t, foundSeriesSelector, "series.selector attribute not found in traces")
	require.True(t, foundResultSeries, "result.series attribute not found in traces")
	require.True(t, foundResultSamples, "result.samples attribute not found in traces")
	require.True(t, foundEstimatedBytes, "result.estimated_bytes attribute not found in traces")
	require.True(t, foundWireBytes, "result.wire_bytes attribute not found in traces")
}
