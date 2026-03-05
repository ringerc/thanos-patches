// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type GRPCAPI struct {
	now                   func() time.Time
	replicaLabels         []string
	queryableCreate       query.QueryableCreator
	remoteEndpointsCreate query.RemoteEndpointsCreator
	queryCreator          queryCreator
	defaultEngine         querypb.EngineType
	lookbackDeltaCreate   func(int64) time.Duration
	defaultMaxResolution  time.Duration
}

func NewGRPCAPI(
	now func() time.Time,
	replicaLabels []string,
	queryableCreator query.QueryableCreator,
	remoteEndpointsCreator query.RemoteEndpointsCreator,
	queryCreator queryCreator,
	defaultEngine querypb.EngineType,
	lookbackDeltaCreate func(int64) time.Duration,
	defaultMaxResolution time.Duration,
) *GRPCAPI {
	return &GRPCAPI{
		now:                   now,
		replicaLabels:         replicaLabels,
		queryableCreate:       queryableCreator,
		remoteEndpointsCreate: remoteEndpointsCreator,
		queryCreator:          queryCreator,
		defaultEngine:         defaultEngine,
		lookbackDeltaCreate:   lookbackDeltaCreate,
		defaultMaxResolution:  defaultMaxResolution,
	}
}

func RegisterQueryServer(queryServer querypb.QueryServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		querypb.RegisterQueryServer(s, queryServer)
	}
}

func (g *GRPCAPI) Query(request *querypb.QueryRequest, server querypb.Query_QueryServer) error {
	ctx := server.Context()

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("query.expr", request.Query)
	}

	if request.TimeoutSeconds != 0 {
		var cancel context.CancelFunc
		timeout := time.Duration(request.TimeoutSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	maxResolution := request.MaxResolutionSeconds
	if request.MaxResolutionSeconds == 0 {
		maxResolution = g.defaultMaxResolution.Milliseconds() / 1000
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}

	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		false,
		request.ShardInfo,
		query.NoopSeriesStatsReporter,
	)

	remoteEndpoints := g.remoteEndpointsCreate(
		replicaLabels,
		request.EnablePartialResponse,
	)

	var qry promql.Query
	if err := tracing.DoInSpanWithErr(ctx, "instant_query_create", func(ctx context.Context) error {
		var err error
		qry, err = g.getInstantQueryForEngine(ctx, request, queryable, remoteEndpoints, maxResolution)
		return err
	}); err != nil {
		return err
	}
	defer qry.Close()

	var result *promql.Result
	tracing.DoInSpan(ctx, "instant_query_exec", func(ctx context.Context) {
		result = qry.Exec(ctx)
	})
	if result.Err != nil {
		if request.EnablePartialResponse {
			if err := server.Send(querypb.NewQueryWarningsResponse(result.Err)); err != nil {
				return err
			}
			return nil
		}
		return status.Error(codes.Aborted, result.Err.Error())
	}

	if len(result.Warnings) != 0 {
		if err := server.Send(querypb.NewQueryWarningsResponse(result.Warnings.AsErrors()...)); err != nil {
			return err
		}
	}

	span := opentracing.SpanFromContext(ctx)
	var numSeries, numSamples, estimatedMemoryBytes int64
	batchSize := request.ResponseBatchSize
	switch vector := result.Value.(type) {
	case promql.Scalar:
		numSeries = 1
		numSamples = 1
		if span != nil {
			estimatedMemoryBytes = 16 // timestamp + value
		}
		series := &prompb.TimeSeries{
			Samples: []prompb.Sample{{Value: vector.V, Timestamp: vector.T}},
		}
		if err := server.Send(querypb.NewQueryResponse(series)); err != nil {
			return err
		}
	case promql.Vector:
		numSeries = int64(len(vector))
		numSamples = int64(len(vector))
		// Estimate memory for each sample (labels + data) only if tracing
		if span != nil {
			for _, sample := range vector {
				estimatedMemoryBytes += estimateLabelsMemory(sample.Metric)
				estimatedMemoryBytes += estimateSampleMemory(sample)
			}
		}
		if batchSize <= 1 {
			for _, sample := range vector {
				floats, histograms := prompb.SamplesFromPromqlSamples(sample)
				series := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				if err := server.Send(querypb.NewQueryResponse(series)); err != nil {
					return err
				}
			}
		} else {
			batch := make([]*prompb.TimeSeries, 0, batchSize)
			for _, sample := range vector {
				floats, histograms := prompb.SamplesFromPromqlSamples(sample)
				series := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				batch = append(batch, series)
				if int64(len(batch)) >= batchSize {
					if err := server.Send(querypb.NewQueryBatchResponse(batch)); err != nil {
						return err
					}
					batch = make([]*prompb.TimeSeries, 0, batchSize)
				}
			}
			if len(batch) > 0 {
				if err := server.Send(querypb.NewQueryBatchResponse(batch)); err != nil {
					return err
				}
			}
		}
	}
	if err := server.Send(querypb.NewQueryStatsResponse(extractQueryStats(qry))); err != nil {
		return err
	}

	if span != nil {
		span.SetTag("result.series", numSeries)
		span.SetTag("result.samples", numSamples)
		span.SetTag("result.estimated_bytes", estimatedMemoryBytes)
	}

	return nil
}

func (g *GRPCAPI) QueryRange(request *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	ctx := srv.Context()

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("query.expr", request.Query)
	}

	if request.TimeoutSeconds != 0 {
		var cancel context.CancelFunc
		timeout := time.Duration(request.TimeoutSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	maxResolution := request.MaxResolutionSeconds
	if request.MaxResolutionSeconds == 0 {
		maxResolution = g.defaultMaxResolution.Milliseconds() / 1000
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}

	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		false,
		request.ShardInfo,
		query.NoopSeriesStatsReporter,
	)

	remoteEndpoints := g.remoteEndpointsCreate(
		replicaLabels,
		request.EnablePartialResponse,
	)

	var qry promql.Query
	if err := tracing.DoInSpanWithErr(ctx, "range_query_create", func(ctx context.Context) error {
		var err error
		qry, err = g.getRangeQueryForEngine(ctx, request, queryable, remoteEndpoints, maxResolution)
		return err
	}); err != nil {
		return err
	}
	defer qry.Close()

	var result *promql.Result
	tracing.DoInSpan(ctx, "range_query_exec", func(ctx context.Context) {
		result = qry.Exec(ctx)
	})
	if result.Err != nil {
		return status.Error(codes.Aborted, result.Err.Error())
	}

	if len(result.Warnings) != 0 {
		if err := srv.Send(querypb.NewQueryRangeWarningsResponse(result.Warnings.AsErrors()...)); err != nil {
			return err
		}
	}

	span := opentracing.SpanFromContext(ctx)
	var numSeries, numSamples, estimatedMemoryBytes int64
	batchSize := request.ResponseBatchSize
	switch value := result.Value.(type) {
	case promql.Matrix:
		numSeries = int64(len(value))
		if span != nil {
			for _, s := range value {
				numSamples += int64(len(s.Floats) + len(s.Histograms))
				estimatedMemoryBytes += estimateSeriesMemory(s)
			}
		} else {
			for _, s := range value {
				numSamples += int64(len(s.Floats) + len(s.Histograms))
			}
		}
		if batchSize <= 1 {
			for _, series := range value {
				floats, histograms := prompb.SamplesFromPromqlSeries(series)
				ts := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(series.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				if err := srv.Send(querypb.NewQueryRangeResponse(ts)); err != nil {
					return err
				}
			}
		} else {
			batch := make([]*prompb.TimeSeries, 0, batchSize)
			for _, series := range value {
				floats, histograms := prompb.SamplesFromPromqlSeries(series)
				ts := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(series.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				batch = append(batch, ts)
				if int64(len(batch)) >= batchSize {
					if err := srv.Send(querypb.NewQueryRangeBatchResponse(batch)); err != nil {
						return err
					}
					batch = make([]*prompb.TimeSeries, 0, batchSize)
				}
			}
			if len(batch) > 0 {
				if err := srv.Send(querypb.NewQueryRangeBatchResponse(batch)); err != nil {
					return err
				}
			}
		}
	case promql.Vector:
		numSeries = int64(len(value))
		numSamples = int64(len(value))
		// Estimate memory for each sample (labels + data) only if tracing
		if span != nil {
			for _, sample := range value {
				estimatedMemoryBytes += estimateLabelsMemory(sample.Metric)
				estimatedMemoryBytes += estimateSampleMemory(sample)
			}
		}
		if batchSize <= 1 {
			for _, sample := range value {
				floats, histograms := prompb.SamplesFromPromqlSamples(sample)
				series := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
					return err
				}
			}
		} else {
			batch := make([]*prompb.TimeSeries, 0, batchSize)
			for _, sample := range value {
				floats, histograms := prompb.SamplesFromPromqlSamples(sample)
				series := &prompb.TimeSeries{
					Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
					Samples:    floats,
					Histograms: histograms,
				}
				batch = append(batch, series)
				if int64(len(batch)) >= batchSize {
					if err := srv.Send(querypb.NewQueryRangeBatchResponse(batch)); err != nil {
						return err
					}
					batch = make([]*prompb.TimeSeries, 0, batchSize)
				}
			}
			if len(batch) > 0 {
				if err := srv.Send(querypb.NewQueryRangeBatchResponse(batch)); err != nil {
					return err
				}
			}
		}
	case promql.Scalar:
		numSeries = 1
		numSamples = 1
		if span != nil {
			estimatedMemoryBytes = 16 // timestamp + value
		}
		series := &prompb.TimeSeries{
			Samples: []prompb.Sample{{Value: value.V, Timestamp: value.T}},
		}
		if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
			return err
		}
	}
	if err := srv.Send(querypb.NewQueryRangeStatsResponse(extractQueryStats(qry))); err != nil {
		return err
	}

	if span != nil {
		span.SetTag("result.series", numSeries)
		span.SetTag("result.samples", numSamples)
		span.SetTag("result.estimated_bytes", estimatedMemoryBytes)
	}

	return nil
}

func extractQueryStats(qry promql.Query) *querypb.QueryStats {
	stats := &querypb.QueryStats{
		SamplesTotal: 0,
		PeakSamples:  0,
	}
	if explQry, ok := qry.(engine.ExplainableQuery); ok {
		analyze := explQry.Analyze()
		if analyze == nil {
			return stats
		}
		stats.SamplesTotal = analyze.TotalSamples()
		stats.PeakSamples = analyze.PeakSamples()
	}

	return stats
}

// estimateLabelsMemory estimates memory usage of label set.
func estimateLabelsMemory(lbls labels.Labels) int64 {
	var size int64
	lbls.Range(func(l labels.Label) {
		size += int64(len(l.Name) + len(l.Value))
	})
	return size
}

// estimateSampleMemory estimates memory usage of a single sample.
// Returns 16 bytes for float samples, or ~40 bytes + bucket data for histograms.
func estimateSampleMemory(sample promql.Sample) int64 {
	if sample.H != nil {
		// Native histogram: schema + metadata (~40 bytes) + buckets (8 bytes each)
		return 40 + int64(len(sample.H.PositiveBuckets)+len(sample.H.NegativeBuckets))*8
	}
	// Float sample: timestamp (8 bytes) + value (8 bytes)
	return 16
}

// estimateSeriesMemory estimates memory usage of a time series including all samples.
func estimateSeriesMemory(series promql.Series) int64 {
	size := estimateLabelsMemory(series.Metric)
	// Float samples: 16 bytes each (timestamp + value)
	size += int64(len(series.Floats)) * 16
	// Histogram samples: estimate based on bucket counts
	for _, h := range series.Histograms {
		if h.H != nil {
			size += 40 + int64(len(h.H.PositiveBuckets)+len(h.H.NegativeBuckets))*8
		}
	}
	return size
}

func (g *GRPCAPI) getInstantQueryForEngine(
	ctx context.Context,
	request *querypb.QueryRequest,
	queryable storage.Queryable,
	remoteEndpoints api.RemoteEndpoints,
	maxResolution int64,
) (promql.Query, error) {
	lookbackDelta := g.lookbackDeltaCreate(maxResolution * 1000)
	if request.LookbackDeltaSeconds > 0 {
		lookbackDelta = time.Duration(request.LookbackDeltaSeconds) * time.Second
	}
	engineParam := request.Engine
	if engineParam == querypb.EngineType_default {
		engineParam = g.defaultEngine
	}

	var ts time.Time
	if request.TimeSeconds == 0 {
		ts = g.now()
	} else {
		ts = time.Unix(request.TimeSeconds, 0)
	}
	opts := &engine.QueryOpts{
		LookbackDeltaParam: lookbackDelta,
	}

	var engineType PromqlEngineType
	switch engineParam {
	case querypb.EngineType_prometheus:
		engineType = PromqlEnginePrometheus
	case querypb.EngineType_thanos:
		engineType = PromqlEngineThanos
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid engine parameter")
	}

	var qry planOrQuery
	if plan, err := logicalplan.Unmarshal(request.QueryPlan.GetJson()); err != nil {
		qry = planOrQuery{plan: plan, query: request.Query}
	} else {
		qry = planOrQuery{query: request.Query}
	}
	return g.queryCreator.makeInstantQuery(ctx, engineType, queryable, remoteEndpoints, qry, opts, ts)
}

func (g *GRPCAPI) getRangeQueryForEngine(
	ctx context.Context,
	request *querypb.QueryRangeRequest,
	queryable storage.Queryable,
	remoteEndpoints api.RemoteEndpoints,
	maxResolution int64,
) (promql.Query, error) {
	start := time.Unix(request.StartTimeSeconds, 0)
	end := time.Unix(request.EndTimeSeconds, 0)
	step := time.Duration(request.IntervalSeconds) * time.Second

	engineParam := request.Engine
	if engineParam == querypb.EngineType_default {
		engineParam = g.defaultEngine
	}

	lookbackDelta := g.lookbackDeltaCreate(maxResolution * 1000)
	if request.LookbackDeltaSeconds > 0 {
		lookbackDelta = time.Duration(request.LookbackDeltaSeconds) * time.Second
	}
	opts := &engine.QueryOpts{
		LookbackDeltaParam: lookbackDelta,
	}

	var engineType PromqlEngineType
	switch engineParam {
	case querypb.EngineType_prometheus:
		engineType = PromqlEnginePrometheus
	case querypb.EngineType_thanos:
		engineType = PromqlEngineThanos
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid engine parameter")
	}

	var qry planOrQuery
	if plan, err := logicalplan.Unmarshal(request.QueryPlan.GetJson()); err != nil {
		qry = planOrQuery{plan: plan, query: request.Query}
	} else {
		qry = planOrQuery{query: request.Query}
	}
	return g.queryCreator.makeRangeQuery(ctx, engineType, queryable, remoteEndpoints, qry, opts, start, end, step)
}
