package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/semaphore"
)

type storeConstructor func(Config) (store, error)

var _ objstore.Bucket = (*Bucket)(nil)

// maxUploadCommitAttempts tolerates seven concurrent winners while bounding one
// upload to sixteen manifest rows: one initial pending, eight commits, and seven rebases.
const maxUploadCommitAttempts = 8

// ErrConcurrentMutation indicates that another generation won while an object was being deleted.
var ErrConcurrentMutation = errors.New("ClickHouse object changed concurrently")

var errUploadCommitAttemptsExhausted = errors.New("ClickHouse upload commit attempts exhausted")

// Bucket stores objects as atomically committed ClickHouse chunk generations.
type Bucket struct {
	cfg              Config
	name             string
	logger           log.Logger
	store            store
	versionClock     *versionClock
	metrics          *providerMetrics
	manifests        *manifestCache
	readBudget       *semaphore.Weighted
	now              func() time.Time
	cleanupPartition uint32
	cleanupCancel    context.CancelFunc
	cleanupDone      chan struct{}
	closeOnce        sync.Once
	closeErr         error
}

// manifestCache memoizes latest live manifests for a short TTL. Objects are
// immutable once committed, so a cached manifest only delays visibility of
// same-key overwrites and deletes; local writers invalidate their key.
// It bounds the query fan-out of ranged reads: without it every ReadAt issues
// a manifest lookup before its chunk query, which under parquet page reads
// exhausts the ClickHouse connection pool.
type manifestCache struct {
	ttl        time.Duration
	maxEntries int
	mu         sync.Mutex
	entries    map[string]manifestCacheEntry
}

type manifestCacheEntry struct {
	value     manifest
	expiresAt time.Time
}

const manifestCacheMaxEntries = 16384

func newManifestCache(ttl time.Duration) *manifestCache {
	if ttl <= 0 {
		return nil
	}
	return &manifestCache{
		ttl:        ttl,
		maxEntries: manifestCacheMaxEntries,
		entries:    make(map[string]manifestCacheEntry),
	}
}

func (c *manifestCache) get(key string, now time.Time) (manifest, bool) {
	if c == nil {
		return manifest{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || now.After(entry.expiresAt) {
		delete(c.entries, key)
		return manifest{}, false
	}
	return entry.value, true
}

func (c *manifestCache) put(key string, value manifest, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxEntries {
		// Full reset keeps the cache bounded without tracking recency; a
		// cold cache only costs one manifest query per live object.
		c.entries = make(map[string]manifestCacheEntry)
	}
	c.entries[key] = manifestCacheEntry{value: value, expiresAt: now.Add(c.ttl)}
}

func (c *manifestCache) invalidate(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// NewBucketClient creates and initializes a ClickHouse object store bucket.
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger, registerers ...prometheus.Registerer) (*Bucket, error) {
	return newBucketClient(ctx, cfg, name, logger, func(cfg Config) (store, error) {
		return newClickhouseStore(cfg)
	}, registerers...)
}

func newBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger, open storeConstructor, registerers ...prometheus.Registerer) (*Bucket, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate ClickHouse bucket configuration: %w", err)
	}
	persistence, err := open(cfg)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse bucket %q: %w", name, err)
	}
	if err := persistence.Init(ctx); err != nil {
		initErr := fmt.Errorf("initialize ClickHouse bucket %q: %w", name, err)
		if closeErr := persistence.Close(); closeErr != nil {
			return nil, errors.Join(initErr, fmt.Errorf("close ClickHouse bucket %q after initialization failure: %w", name, closeErr))
		}
		return nil, initErr
	}
	bucket, err := newBucketWithStore(cfg, name, logger, persistence, registerers...)
	if err != nil {
		metricsErr := fmt.Errorf("initialize ClickHouse bucket %q metrics: %w", name, err)
		if closeErr := persistence.Close(); closeErr != nil {
			return nil, errors.Join(metricsErr, fmt.Errorf("close ClickHouse bucket %q after metrics failure: %w", name, closeErr))
		}
		return nil, metricsErr
	}
	return bucket, nil
}

func newBucketWithStore(cfg Config, name string, logger log.Logger, persistence store, registerers ...prometheus.Registerer) (*Bucket, error) {
	var registerer prometheus.Registerer
	if len(registerers) > 0 {
		registerer = registerers[0]
	}
	return newBucketWithStoreOptions(cfg, name, logger, persistence, bucketOptions{registerer: registerer})
}

func newBucketWithStoreOptions(cfg Config, name string, logger log.Logger, persistence store, options bucketOptions) (*Bucket, error) {
	if options.now == nil {
		options.now = time.Now
	}
	if options.newTicker == nil {
		options.newTicker = func(interval time.Duration) cleanupTicker {
			return realCleanupTicker{Ticker: time.NewTicker(interval)}
		}
	}
	metrics, err := newProviderMetrics(name, options.registerer)
	if err != nil {
		return nil, err
	}
	bucket := &Bucket{
		cfg:          cfg,
		name:         name,
		logger:       logger,
		store:        persistence,
		versionClock: newVersionClock(options.now),
		metrics:      metrics,
		manifests:    newManifestCache(cfg.ManifestCacheTTL),
		now:          options.now,
	}
	if cfg.MaxInflightReadBytes > 0 {
		// Bounds the chunk payload bytes all readers hold at once, so wide
		// query fan-outs queue instead of exhausting process memory.
		bucket.readBudget = semaphore.NewWeighted(int64(cfg.MaxInflightReadBytes))
	}
	bucket.startCleanup(options.newTicker)
	return bucket, nil
}

// Upload writes a new object generation and commits it only after every chunk is durable.
func (b *Bucket) Upload(ctx context.Context, key string, reader io.Reader, _ ...objstore.ObjectUploadOption) (err error) {
	finish := b.metrics.startOperation("upload")
	defer func() { finish(err) }()

	if key == "" {
		return errors.New("upload ClickHouse object: object key must not be empty")
	}
	if err := b.cfg.validateUploadAllocations(); err != nil {
		return fmt.Errorf("upload ClickHouse object %q: validate allocations: %w", key, err)
	}

	uploadCtx, cancel := context.WithTimeout(ctx, b.cfg.MaxUploadDuration)
	defer cancel()
	if err := uploadCtx.Err(); err != nil {
		return fmt.Errorf("upload ClickHouse object %q: start upload: %w", key, err)
	}

	version, err := b.nextUploadVersion(uploadCtx, key)
	if err != nil {
		return fmt.Errorf("upload ClickHouse object %q: allocate version: %w", key, err)
	}
	upload, err := b.store.BeginUpload(uploadCtx, manifest{
		Key:        key,
		Generation: uuid.New(),
		ChunkSize:  uint32(b.cfg.ChunkSize),
		Version:    version,
	})
	if err != nil {
		return fmt.Errorf("upload ClickHouse object %q: begin upload: %w", key, err)
	}

	buffer := make([]byte, b.cfg.ChunkSize)
	batch := make([]chunk, 0, b.cfg.InsertBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := b.store.InsertChunks(uploadCtx, batch); err != nil {
			return fmt.Errorf("upload ClickHouse object %q: insert chunks: %w", key, err)
		}
		var bytes int
		for _, value := range batch {
			bytes += len(value.Data)
		}
		b.metrics.uploadChunks.Add(float64(len(batch)))
		b.metrics.uploadBytes.Add(float64(bytes))
		batch = batch[:0]
		return nil
	}

	for {
		if err := uploadCtx.Err(); err != nil {
			return fmt.Errorf("upload ClickHouse object %q: read chunks: %w", key, err)
		}
		n, readErr := io.ReadFull(reader, buffer)
		if n > 0 {
			index := upload.ChunkCount
			if err := checkedUploadProgress(&upload, n); err != nil {
				return fmt.Errorf("upload ClickHouse object %q: account for chunk: %w", key, err)
			}
			batch = append(batch, chunk{
				Key:        key,
				Generation: upload.Generation,
				Index:      index,
				Data:       append([]byte(nil), buffer[:n]...),
			})
			if len(batch) == b.cfg.InsertBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		switch {
		case readErr == nil:
			continue
		case errors.Is(readErr, io.EOF), errors.Is(readErr, io.ErrUnexpectedEOF):
			if err := flush(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("upload ClickHouse object %q: read chunk: %w", key, readErr)
		}
		break
	}

	if err := uploadCtx.Err(); err != nil {
		return fmt.Errorf("upload ClickHouse object %q: commit upload: %w", key, err)
	}
	if err := b.commitVisibleUpload(uploadCtx, upload); err != nil {
		return fmt.Errorf("upload ClickHouse object %q: commit upload: %w", key, err)
	}
	b.manifests.invalidate(key)
	return nil
}

func (b *Bucket) nextUploadVersion(ctx context.Context, key string) (uint64, error) {
	latest, err := b.store.LatestManifest(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return b.versionClock.Next()
	}
	if err != nil {
		return 0, fmt.Errorf("read persisted latest generation: %w", err)
	}
	return b.nextVersionAfter(latest.Version)
}

func (b *Bucket) commitVisibleUpload(ctx context.Context, upload manifest) error {
	// Concurrent writers can choose the same version. Verify the persisted winner
	// and rebase only this generation's manifest until it is visible or bounded out.
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.store.CommitUpload(ctx, upload); err != nil {
			return err
		}

		winner, err := b.store.LatestManifest(ctx, upload.Key)
		if err != nil {
			return fmt.Errorf("verify committed generation: %w", err)
		}
		if winner.Generation == upload.Generation && winner.State == committed {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt == maxUploadCommitAttempts {
			return fmt.Errorf("%w after %d attempts", errUploadCommitAttemptsExhausted, maxUploadCommitAttempts)
		}
		b.metrics.commitRetries.Inc()

		version, err := b.nextVersionAfter(winner.Version)
		if err != nil {
			return fmt.Errorf("rebase committed generation after concurrent winner: %w", err)
		}
		upload.Version = version
		upload, err = b.store.BeginUpload(ctx, upload)
		if err != nil {
			return fmt.Errorf("rebase committed generation after concurrent winner: %w", err)
		}
	}
}

func checkedUploadProgress(upload *manifest, chunkBytes int) error {
	if chunkBytes < 0 || uint64(chunkBytes) > math.MaxUint64-upload.Size {
		return errors.New("object size overflows uint64")
	}
	if upload.ChunkCount == math.MaxUint32 {
		return errors.New("chunk count overflows uint32")
	}
	upload.Size += uint64(chunkBytes)
	upload.ChunkCount++
	return nil
}

// Delete makes the latest live object generation invisible by appending a tombstone.
// It verifies that the tombstone is latest before returning. A write that linearizes
// after this postcondition may recreate the key; same-key concurrent mutation is
// last-version-wins and is not globally linearizable.
func (b *Bucket) Delete(ctx context.Context, key string) (err error) {
	finish := b.metrics.startOperation("delete")
	defer func() { finish(err) }()

	if key == "" {
		return errors.New("delete ClickHouse object: object key must not be empty")
	}
	// Version allocation must see the persisted latest manifest, not a
	// cached one; the local cache is invalidated once the tombstone lands.
	latest, err := b.latestLiveManifestUncached(ctx, key)
	if err != nil {
		return fmt.Errorf("delete ClickHouse object %q: %w", key, err)
	}
	version, err := b.nextVersionAfter(latest.Version)
	if err != nil {
		return fmt.Errorf("delete ClickHouse object %q: allocate version: %w", key, err)
	}

	tombstone := latest
	tombstone.Generation = uuid.New()
	tombstone.State = deleted
	tombstone.Version = version
	tombstone.LeaseExpiresAt = time.Time{}
	tombstone.EventAt = time.Time{}
	if _, err := b.store.InsertDelete(ctx, tombstone); err != nil {
		return fmt.Errorf("delete ClickHouse object %q: insert tombstone: %w", key, err)
	}
	winner, err := b.store.LatestManifest(ctx, key)
	if err != nil {
		return fmt.Errorf("delete ClickHouse object %q: verify tombstone: %w", key, err)
	}
	if winner.Generation != tombstone.Generation || winner.State != deleted {
		return fmt.Errorf(
			"delete ClickHouse object %q: %w: tombstone generation %s lost to generation %s state %s version %d",
			key, ErrConcurrentMutation, tombstone.Generation, winner.Generation, winner.State, winner.Version,
		)
	}
	b.manifests.invalidate(key)
	return nil
}

func (b *Bucket) nextVersionAfter(current uint64) (uint64, error) {
	if current == math.MaxUint64 {
		return 0, errors.New("ClickHouse version clock is exhausted")
	}
	return current + 1, nil
}

func (b *Bucket) latestLiveManifest(ctx context.Context, key string) (manifest, error) {
	if cached, ok := b.manifests.get(key, b.now()); ok {
		return cached, nil
	}
	latest, err := b.latestLiveManifestUncached(ctx, key)
	if err != nil {
		return manifest{}, err
	}
	b.manifests.put(key, latest, b.now())
	return latest, nil
}

func (b *Bucket) latestLiveManifestUncached(ctx context.Context, key string) (manifest, error) {
	if key == "" {
		return manifest{}, errors.New("read ClickHouse object: object key must not be empty")
	}
	latest, err := b.store.LatestManifest(ctx, key)
	if err != nil {
		return manifest{}, fmt.Errorf("read ClickHouse object %q manifest: %w", key, err)
	}
	if latest.State == deleted {
		return manifest{}, fmt.Errorf("read ClickHouse object %q: latest generation is deleted: %w", key, ErrObjectNotFound)
	}
	return latest, nil
}

// Close closes the underlying ClickHouse store once.
func (b *Bucket) Close() error {
	b.closeOnce.Do(func() {
		if b.cleanupCancel != nil {
			b.cleanupCancel()
			<-b.cleanupDone
		}
		b.closeErr = b.store.Close()
	})
	return b.closeErr
}

// Name returns the configured bucket name.
func (b *Bucket) Name() string {
	return b.name
}

// Provider returns the ClickHouse object store provider name.
func (b *Bucket) Provider() objstore.ObjProvider {
	return objstore.ObjProvider("CLICKHOUSE")
}
