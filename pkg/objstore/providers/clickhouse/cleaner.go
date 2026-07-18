package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type cleanupTicker interface {
	C() <-chan time.Time
	Stop()
}

type realCleanupTicker struct {
	*time.Ticker
}

func (t realCleanupTicker) C() <-chan time.Time { return t.Ticker.C }

type bucketOptions struct {
	registerer prometheus.Registerer
	now        func() time.Time
	newTicker  func(time.Duration) cleanupTicker
}

var clickHouseDurationBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800,
}

type providerMetrics struct {
	uploadChunks            prometheus.Counter
	uploadBytes             prometheus.Counter
	chunkQueries            prometheus.Counter
	readPrefetchBytes       prometheus.Counter
	commitRetries           prometheus.Counter
	cleanupCandidates       prometheus.Counter
	cleanupDeletions        prometheus.Counter
	cleanupFailures         prometheus.Counter
	corruptReads            prometheus.Counter
	operationDuration       *prometheus.HistogramVec
	operationFailures       *prometheus.CounterVec
	operationsInflight      *prometheus.GaugeVec
	cleanupDuration         prometheus.Histogram
	cleanupMutationDuration prometheus.Histogram
}

func newProviderMetrics(bucket string, registerer prometheus.Registerer) (_ *providerMetrics, err error) {
	labels := prometheus.Labels{"bucket": bucket}
	metrics := &providerMetrics{
		uploadChunks: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_upload_chunks_total",
			Help:        "Total ClickHouse object chunks durably inserted.",
			ConstLabels: labels,
		}),
		uploadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_upload_bytes_total",
			Help:        "Total ClickHouse object chunk bytes durably inserted.",
			ConstLabels: labels,
		}),
		chunkQueries: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_chunk_queries_total",
			Help:        "Total ClickHouse chunk range queries issued by object readers.",
			ConstLabels: labels,
		}),
		readPrefetchBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_read_prefetch_bytes_total",
			Help:        "Total chunk bytes returned by ClickHouse reader prefetch queries.",
			ConstLabels: labels,
		}),
		commitRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_commit_retries_total",
			Help:        "Total upload commit retries after a concurrent generation won.",
			ConstLabels: labels,
		}),
		cleanupCandidates: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_cleanup_candidates_total",
			Help:        "Total ClickHouse object generations selected for cleanup.",
			ConstLabels: labels,
		}),
		cleanupDeletions: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_cleanup_deletions_total",
			Help:        "Total ClickHouse object generations deleted by cleanup.",
			ConstLabels: labels,
		}),
		cleanupFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_cleanup_failures_total",
			Help:        "Total failed ClickHouse object cleanup operations.",
			ConstLabels: labels,
		}),
		corruptReads: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_corrupt_reads_total",
			Help:        "Total ClickHouse object read operations that detected corrupt data.",
			ConstLabels: labels,
		}),
		operationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pyroscope_objstore_clickhouse_operation_duration_seconds",
			Help:        "Duration of ClickHouse object-store API operations in seconds.",
			ConstLabels: labels,
			Buckets:     clickHouseDurationBuckets,
		}, []string{"operation"}),
		operationFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pyroscope_objstore_clickhouse_operation_failures_total",
			Help:        "Total failed ClickHouse object-store API operations.",
			ConstLabels: labels,
		}, []string{"operation"}),
		operationsInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pyroscope_objstore_clickhouse_operations_inflight",
			Help:        "Current in-flight ClickHouse object-store API operations.",
			ConstLabels: labels,
		}, []string{"operation"}),
		cleanupDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "pyroscope_objstore_clickhouse_cleanup_duration_seconds",
			Help:        "Duration of ClickHouse object cleanup passes in seconds.",
			ConstLabels: labels,
			Buckets:     clickHouseDurationBuckets,
		}),
		cleanupMutationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "pyroscope_objstore_clickhouse_cleanup_mutation_duration_seconds",
			Help:        "Duration of synchronous ClickHouse cleanup mutation batches in seconds.",
			ConstLabels: labels,
			Buckets:     clickHouseDurationBuckets,
		}),
	}
	if registerer == nil {
		return metrics, nil
	}
	registered := make([]prometheus.Collector, 0, 14)
	defer func() {
		if err == nil {
			return
		}
		for i := len(registered) - 1; i >= 0; i-- {
			registerer.Unregister(registered[i])
		}
	}()

	metrics.uploadChunks, err = registerMetric(registerer, metrics.uploadChunks, &registered)
	if err != nil {
		return nil, err
	}
	metrics.uploadBytes, err = registerMetric(registerer, metrics.uploadBytes, &registered)
	if err != nil {
		return nil, err
	}
	metrics.chunkQueries, err = registerMetric(registerer, metrics.chunkQueries, &registered)
	if err != nil {
		return nil, err
	}
	metrics.readPrefetchBytes, err = registerMetric(registerer, metrics.readPrefetchBytes, &registered)
	if err != nil {
		return nil, err
	}
	metrics.commitRetries, err = registerMetric(registerer, metrics.commitRetries, &registered)
	if err != nil {
		return nil, err
	}
	metrics.cleanupCandidates, err = registerMetric(registerer, metrics.cleanupCandidates, &registered)
	if err != nil {
		return nil, err
	}
	metrics.cleanupDeletions, err = registerMetric(registerer, metrics.cleanupDeletions, &registered)
	if err != nil {
		return nil, err
	}
	metrics.cleanupFailures, err = registerMetric(registerer, metrics.cleanupFailures, &registered)
	if err != nil {
		return nil, err
	}
	metrics.corruptReads, err = registerMetric(registerer, metrics.corruptReads, &registered)
	if err != nil {
		return nil, err
	}
	metrics.operationDuration, err = registerMetric(registerer, metrics.operationDuration, &registered)
	if err != nil {
		return nil, err
	}
	metrics.operationFailures, err = registerMetric(registerer, metrics.operationFailures, &registered)
	if err != nil {
		return nil, err
	}
	metrics.operationsInflight, err = registerMetric(registerer, metrics.operationsInflight, &registered)
	if err != nil {
		return nil, err
	}
	metrics.cleanupDuration, err = registerMetric(registerer, metrics.cleanupDuration, &registered)
	if err != nil {
		return nil, err
	}
	metrics.cleanupMutationDuration, err = registerMetric(registerer, metrics.cleanupMutationDuration, &registered)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func (m *providerMetrics) startOperation(operation string) func(error) {
	started := time.Now()
	inflight := m.operationsInflight.WithLabelValues(operation)
	inflight.Inc()
	return func(err error) {
		inflight.Dec()
		m.operationDuration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
		if err != nil {
			m.operationFailures.WithLabelValues(operation).Inc()
		}
	}
}

func registerMetric[T prometheus.Collector](registerer prometheus.Registerer, collector T, registered *[]prometheus.Collector) (T, error) {
	err := registerer.Register(collector)
	if err == nil {
		*registered = append(*registered, collector)
		return collector, nil
	}
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(T); ok && sameMetricKind(collector, existing) {
			return existing, nil
		}
		var zero T
		return zero, fmt.Errorf("register ClickHouse metric: incompatible existing collector %T", already.ExistingCollector)
	}
	var zero T
	return zero, fmt.Errorf("register ClickHouse metric: %w", err)
}

func sameMetricKind(expected, existing prometheus.Collector) bool {
	switch expected.(type) {
	case *prometheus.CounterVec:
		_, ok := existing.(*prometheus.CounterVec)
		return ok
	case *prometheus.GaugeVec:
		_, ok := existing.(*prometheus.GaugeVec)
		return ok
	case *prometheus.HistogramVec:
		_, ok := existing.(*prometheus.HistogramVec)
		return ok
	}
	expectedKind, expectedOK := metricKind(expected)
	existingKind, existingOK := metricKind(existing)
	return expectedOK && existingOK && expectedKind == existingKind
}

func metricKind(collector prometheus.Collector) (string, bool) {
	metric, ok := collector.(prometheus.Metric)
	if !ok {
		return "", false
	}
	var value dto.Metric
	if err := metric.Write(&value); err != nil {
		return "", false
	}
	switch {
	case value.Counter != nil:
		return "counter", true
	case value.Gauge != nil:
		return "gauge", true
	case value.Histogram != nil:
		return "histogram", true
	case value.Summary != nil:
		return "summary", true
	case value.Untyped != nil:
		return "untyped", true
	default:
		return "", false
	}
}

func (b *Bucket) startCleanup(newTicker func(time.Duration) cleanupTicker) {
	if !b.cfg.Cleanup.Enabled {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cleanupCancel = cancel
	b.cleanupDone = make(chan struct{})
	ticker := newTicker(b.cfg.Cleanup.Interval)
	go b.runCleanup(ctx, ticker)
}

func (b *Bucket) runCleanup(ctx context.Context, ticker cleanupTicker) {
	defer close(b.cleanupDone)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			b.cleanupPass(ctx)
		}
	}
}

func (b *Bucket) cleanupPass(ctx context.Context) {
	started := time.Now()
	defer func() {
		b.metrics.cleanupDuration.Observe(time.Since(started).Seconds())
	}()

	partitionCount := uint32(max(b.cfg.PartitionCount, 1))
	startPartition := b.cleanupPartition % partitionCount
	remaining := b.cfg.Cleanup.MutationBatchSize
	for scanned := uint32(0); scanned < partitionCount && remaining > 0; scanned++ {
		partition := (startPartition + scanned) % partitionCount
		b.cleanupPartition = (partition + 1) % partitionCount
		candidates, err := b.store.CleanupCandidates(ctx, partition, b.cfg.Cleanup.Grace, remaining)
		if err != nil {
			if ctx.Err() == nil {
				b.metrics.cleanupFailures.Inc()
				level.Warn(b.logger).Log("msg", "failed to select ClickHouse cleanup candidates", "partition", partition, "err", err)
			}
			return
		}
		if len(candidates) > remaining {
			candidates = candidates[:remaining]
		}
		b.metrics.cleanupCandidates.Add(float64(len(candidates)))
		if len(candidates) == 0 {
			continue
		}
		mutationStarted := time.Now()
		err = b.store.DeleteGenerations(ctx, partition, candidates)
		b.metrics.cleanupMutationDuration.Observe(time.Since(mutationStarted).Seconds())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.metrics.cleanupFailures.Inc()
			level.Warn(b.logger).Log("msg", "failed to delete ClickHouse object generations", "partition", partition, "err", err)
			return
		}
		b.metrics.cleanupDeletions.Add(float64(len(candidates)))
		remaining -= len(candidates)
	}
}
