package clickhouse

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type manualCleanupTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
}

type failingRegisterer struct {
	registry    *prometheus.Registry
	failAt      int
	registers   int
	unregisters int
}

func (r *failingRegisterer) Register(collector prometheus.Collector) error {
	r.registers++
	if r.registers == r.failAt {
		return errors.New("register failed")
	}
	return r.registry.Register(collector)
}

func (r *failingRegisterer) MustRegister(collectors ...prometheus.Collector) {
	for _, collector := range collectors {
		if err := r.Register(collector); err != nil {
			panic(err)
		}
	}
}

func (r *failingRegisterer) Unregister(collector prometheus.Collector) bool {
	r.unregisters++
	return r.registry.Unregister(collector)
}

func newManualCleanupTicker() *manualCleanupTicker {
	return &manualCleanupTicker{ticks: make(chan time.Time, 2), stopped: make(chan struct{})}
}

func (t *manualCleanupTicker) C() <-chan time.Time { return t.ticks }
func (t *manualCleanupTicker) Stop()               { close(t.stopped) }

func cleanupTestConfig() Config {
	cfg := bucketTestConfig()
	cfg.Cleanup = CleanupConfig{
		Enabled:           true,
		Interval:          time.Minute,
		Grace:             2 * time.Minute,
		MutationBatchSize: 2,
	}
	return cfg
}

func TestNewProviderMetricsRollsBackRegistrationFailure(t *testing.T) {
	registerer := &failingRegisterer{registry: prometheus.NewRegistry(), failAt: 2}

	metrics, err := newProviderMetrics("test", registerer)

	require.Nil(t, metrics)
	require.ErrorContains(t, err, "register failed")
	require.Equal(t, 2, registerer.registers)
	require.Equal(t, 1, registerer.unregisters)
	families, gatherErr := registerer.registry.Gather()
	require.NoError(t, gatherErr)
	require.Empty(t, families)
}

func TestNewProviderMetricsRejectsIncompatibleDuplicate(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "pyroscope_objstore_clickhouse_upload_chunks_total",
		Help:        "Total ClickHouse object chunks durably inserted.",
		ConstLabels: prometheus.Labels{"bucket": "test"},
	}))

	metrics, err := newProviderMetrics("test", registry)

	require.Nil(t, metrics)
	require.ErrorContains(t, err, "incompatible")
}

func TestBucketConstructionDoesNotStartCleanupWhenMetricsFail(t *testing.T) {
	registerer := &failingRegisterer{registry: prometheus.NewRegistry(), failAt: 1}
	tickerCalls := 0

	bucket, err := newBucketWithStoreOptions(cleanupTestConfig(), "test", log.NewNopLogger(), &fakeStore{}, bucketOptions{
		registerer: registerer,
		newTicker: func(time.Duration) cleanupTicker {
			tickerCalls++
			return newManualCleanupTicker()
		},
	})

	require.Nil(t, bucket)
	require.Error(t, err)
	require.Zero(t, tickerCalls)
}

func TestBucketCleanupStartsOnFirstTickAndIgnoresClientClock(t *testing.T) {
	ticker := newManualCleanupTicker()
	type cleanupCall struct {
		grace time.Duration
		limit int
	}
	called := make(chan cleanupCall, 1)
	store := &fakeStore{cleanupFn: func(_ context.Context, grace time.Duration, limit int) ([]cleanupCandidate, error) {
		called <- cleanupCall{grace: grace, limit: limit}
		return nil, nil
	}}
	bucket, err := newBucketWithStoreOptions(cleanupTestConfig(), "test", log.NewNopLogger(), store, bucketOptions{
		now:       func() time.Time { panic("cleanup consulted the client clock") },
		newTicker: func(time.Duration) cleanupTicker { return ticker },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })

	select {
	case <-called:
		t.Fatal("cleanup ran during bucket construction")
	default:
	}
	ticker.ticks <- time.Time{}
	call := <-called
	require.Equal(t, 2*time.Minute, call.grace)
	require.Equal(t, 2, call.limit)
}

func TestBucketCleanupIsDisabledWithoutTicker(t *testing.T) {
	cfg := cleanupTestConfig()
	cfg.Cleanup.Enabled = false
	var tickerCalls atomic.Int32
	bucket, err := newBucketWithStoreOptions(cfg, "test", log.NewNopLogger(), &fakeStore{}, bucketOptions{
		newTicker: func(time.Duration) cleanupTicker {
			tickerCalls.Add(1)
			return newManualCleanupTicker()
		},
	})
	require.NoError(t, err)
	require.Zero(t, tickerCalls.Load())
	require.NoError(t, bucket.Close())
}

func TestBucketCleanupPassesDoNotOverlap(t *testing.T) {
	ticker := newManualCleanupTicker()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	store := &fakeStore{cleanupFn: func(context.Context, time.Duration, int) ([]cleanupCandidate, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}}
	bucket, err := newBucketWithStoreOptions(cleanupTestConfig(), "test", log.NewNopLogger(), store, bucketOptions{
		newTicker: func(time.Duration) cleanupTicker { return ticker },
	})
	require.NoError(t, err)
	ticker.ticks <- time.Now()
	<-entered
	ticker.ticks <- time.Now()
	select {
	case <-entered:
		t.Fatal("cleanup passes overlapped")
	default:
	}
	close(release)
	<-entered
	require.NoError(t, bucket.Close())
}

func TestBucketCleanupReportsOneFailedBatch(t *testing.T) {
	ticker := newManualCleanupTicker()
	registry := prometheus.NewRegistry()
	first := cleanupCandidate{Key: "a", Generation: uuid.New()}
	second := cleanupCandidate{Key: "b", Generation: uuid.New()}
	deleted := make(chan []cleanupCandidate, 1)
	store := &fakeStore{
		cleanupFn: func(context.Context, time.Duration, int) ([]cleanupCandidate, error) {
			return []cleanupCandidate{first, second}, nil
		},
		cleanupDel: func(_ context.Context, candidates []cleanupCandidate) error {
			deleted <- candidates
			return errors.New("mutation failed")
		},
	}
	bucket, err := newBucketWithStoreOptions(cleanupTestConfig(), "metrics-bucket", log.NewNopLogger(), store, bucketOptions{
		registerer: registry,
		newTicker:  func(time.Duration) cleanupTicker { return ticker },
	})
	require.NoError(t, err)
	ticker.ticks <- time.Now()
	call := <-deleted
	require.Equal(t, []cleanupCandidate{first, second}, call)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(bucket.metrics.cleanupFailures) == 1
	}, time.Second, time.Millisecond)
	require.NoError(t, bucket.Close())
	require.Equal(t, float64(2), testutil.ToFloat64(bucket.metrics.cleanupCandidates))
	require.Equal(t, float64(0), testutil.ToFloat64(bucket.metrics.cleanupDeletions))
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.cleanupFailures))
	var mutationMetric dto.Metric
	require.NoError(t, bucket.metrics.cleanupMutationDuration.Write(&mutationMetric))
	require.Equal(t, uint64(1), mutationMetric.Histogram.GetSampleCount())
}

func TestBucketCleanupBoundsDefensiveDeletionBatch(t *testing.T) {
	var deleted []cleanupCandidate
	candidates := []cleanupCandidate{
		{Key: "a", Generation: uuid.New()},
		{Key: "b", Generation: uuid.New()},
		{Key: "c", Generation: uuid.New()},
	}
	store := &fakeStore{
		cleanupFn: func(context.Context, time.Duration, int) ([]cleanupCandidate, error) {
			return candidates, nil
		},
		cleanupDel: func(_ context.Context, candidates []cleanupCandidate) error {
			deleted = append(deleted, candidates...)
			return nil
		},
	}
	cfg := cleanupTestConfig()
	cfg.Cleanup.Enabled = false
	bucket, err := newBucketWithStore(cfg, "test", log.NewNopLogger(), store)
	require.NoError(t, err)
	bucket.cleanupPass(context.Background())

	require.Equal(t, candidates[:2], deleted)
	require.Equal(t, float64(2), testutil.ToFloat64(bucket.metrics.cleanupDeletions))
}

func TestBucketCleanupRunsOneScanAndOneDeletionPerPass(t *testing.T) {
	cfg := cleanupTestConfig()
	cfg.Cleanup.Enabled = false
	cfg.Cleanup.MutationBatchSize = 2
	var limits []int
	var batches [][]cleanupCandidate
	store := &fakeStore{
		cleanupFn: func(_ context.Context, _ time.Duration, limit int) ([]cleanupCandidate, error) {
			limits = append(limits, limit)
			return []cleanupCandidate{
				{Key: "a", Generation: uuid.New()},
				{Key: "b", Generation: uuid.New()},
			}, nil
		},
		cleanupDel: func(_ context.Context, candidates []cleanupCandidate) error {
			batches = append(batches, candidates)
			return nil
		},
	}
	bucket, err := newBucketWithStore(cfg, "test", log.NewNopLogger(), store)
	require.NoError(t, err)

	bucket.cleanupPass(context.Background())
	bucket.cleanupPass(context.Background())

	// One candidates scan with the full batch budget and exactly one
	// deletion batch per pass.
	require.Equal(t, []int{2, 2}, limits)
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 2)
	require.Len(t, batches[1], 2)
	require.Equal(t, float64(4), testutil.ToFloat64(bucket.metrics.cleanupDeletions))
}

func TestBucketCleanupSelectionFailureAndDurationMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	cfg := cleanupTestConfig()
	cfg.Cleanup.Enabled = false
	bucket, err := newBucketWithStore(cfg, "observed", log.NewNopLogger(), &fakeStore{
		cleanupErr: errors.New("selection failed"),
	}, registry)
	require.NoError(t, err)

	bucket.cleanupPass(context.Background())
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.cleanupFailures))

	families, err := registry.Gather()
	require.NoError(t, err)
	var found bool
	for _, family := range families {
		if family.GetName() != "pyroscope_objstore_clickhouse_cleanup_duration_seconds" {
			continue
		}
		found = true
		require.Equal(t, uint64(1), family.Metric[0].Histogram.GetSampleCount())
		require.Equal(t, "bucket", family.Metric[0].Label[0].GetName())
		require.Equal(t, "observed", family.Metric[0].Label[0].GetValue())
	}
	require.True(t, found)
}

func TestBucketCloseCancelsCleanupBeforeClosingStore(t *testing.T) {
	ticker := newManualCleanupTicker()
	entered := make(chan struct{})
	store := &fakeStore{
		cleanupFn: func(context.Context, time.Duration, int) ([]cleanupCandidate, error) {
			return []cleanupCandidate{{Key: "key", Generation: uuid.New()}}, nil
		},
		cleanupDel: func(ctx context.Context, _ []cleanupCandidate) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	bucket, err := newBucketWithStoreOptions(cleanupTestConfig(), "test", log.NewNopLogger(), store, bucketOptions{
		newTicker: func(time.Duration) cleanupTicker { return ticker },
	})
	require.NoError(t, err)
	ticker.ticks <- time.Now()
	<-entered

	closed := make(chan error, 1)
	go func() { closed <- bucket.Close() }()
	require.NoError(t, <-closed)
	require.Equal(t, 1, store.closeCalls)
	select {
	case <-ticker.stopped:
	default:
		t.Fatal("cleanup ticker was not stopped")
	}
}
