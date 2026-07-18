package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/thanos-io/objstore"
)

// Get opens a reader pinned to the latest live object generation.
func (b *Bucket) Get(ctx context.Context, key string) (_ io.ReadCloser, err error) {
	finish := b.metrics.startOperation("get")
	defer func() { finish(err) }()
	return b.openReader(ctx, key, 0, -1)
}

// GetRange opens a reader pinned to the latest live object generation and logical byte range.
func (b *Bucket) GetRange(ctx context.Context, key string, offset, length int64) (_ io.ReadCloser, err error) {
	finish := b.metrics.startOperation("get_range")
	defer func() { finish(err) }()

	if offset < 0 {
		return nil, errors.New("read ClickHouse object range: offset must not be negative")
	}
	if length < -1 {
		return nil, errors.New("read ClickHouse object range: length must be -1 or non-negative")
	}
	return b.openReader(ctx, key, offset, length)
}

func (b *Bucket) openReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	object, err := b.latestLiveManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := validateReadableManifest(object); err != nil {
		b.reportCorruption(err)
		return nil, err
	}

	start := uint64(offset)
	if start > object.Size {
		start = object.Size
	}
	remaining := object.Size - start
	logicalSize := remaining
	if length >= 0 && uint64(length) < logicalSize {
		logicalSize = uint64(length)
	}

	return newChunkReader(ctx, b.store, b.metrics, object, start, logicalSize, b.cfg.ReadPrefetchChunks), nil
}

// Attributes returns the committed manifest's object size and event time.
func (b *Bucket) Attributes(ctx context.Context, key string) (_ objstore.ObjectAttributes, err error) {
	finish := b.metrics.startOperation("attributes")
	defer func() { finish(err) }()

	object, err := b.latestLiveManifest(ctx, key)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	if err := validateReadableManifest(object); err != nil {
		b.reportCorruption(err)
		return objstore.ObjectAttributes{}, err
	}
	return objstore.ObjectAttributes{
		Size:         int64(object.Size),
		LastModified: object.EventAt,
	}, nil
}

func (b *Bucket) reportCorruption(err error) {
	var corruption *CorruptionError
	if errors.As(err, &corruption) {
		b.metrics.corruptReads.Inc()
	}
}

// Exists reports whether the latest object operation is live.
func (b *Bucket) Exists(ctx context.Context, key string) (_ bool, err error) {
	finish := b.metrics.startOperation("exists")
	defer func() { finish(err) }()

	_, err = b.latestLiveManifest(ctx, key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrObjectNotFound) {
		return false, nil
	}
	return false, err
}

// IsObjNotFoundErr reports whether err wraps the ClickHouse not-found sentinel.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return errors.Is(err, ErrObjectNotFound)
}

// IsAccessDeniedErr does not classify arbitrary ClickHouse errors as access denied.
func (b *Bucket) IsAccessDeniedErr(error) bool {
	return false
}

func validateReadableManifest(object manifest) error {
	if object.Size > math.MaxInt64 {
		return corruptManifest(object, "object size exceeds int64")
	}
	if object.Size == 0 {
		if object.ChunkCount != 0 {
			return corruptManifest(object, fmt.Sprintf("empty object has %d chunks", object.ChunkCount))
		}
		return nil
	}
	if object.ChunkSize == 0 {
		return corruptManifest(object, "non-empty object has zero chunk size")
	}
	expected := 1 + (object.Size-1)/uint64(object.ChunkSize)
	if expected != uint64(object.ChunkCount) {
		return corruptManifest(object, fmt.Sprintf("manifest chunk count is %d, expected %d", object.ChunkCount, expected))
	}
	return nil
}

func corruptManifest(object manifest, reason string) error {
	return &CorruptionError{Key: object.Key, Generation: object.Generation, Reason: reason}
}

type chunkReader struct {
	ctx      context.Context
	cancel   context.CancelFunc
	store    store
	metrics  *providerMetrics
	manifest manifest
	start    uint64
	end      uint64
	prefetch uint32
	mu       sync.Mutex
	next     uint32
	last     uint32
	batch    []chunk
	batchPos int
	buffer   []byte
	terminal *CorruptionError
	done     bool
	closed   bool
}

func newChunkReader(ctx context.Context, persistence store, metrics *providerMetrics, object manifest, start, size uint64, prefetch int) *chunkReader {
	readerCtx, cancel := context.WithCancel(ctx)
	reader := &chunkReader{
		ctx:      readerCtx,
		cancel:   cancel,
		store:    persistence,
		metrics:  metrics,
		manifest: object,
		start:    start,
		end:      start + size,
		prefetch: uint32(prefetch),
		done:     size == 0,
	}
	if size > 0 {
		reader.next = uint32(start / uint64(object.ChunkSize))
		reader.last = uint32((reader.end - 1) / uint64(object.ChunkSize))
	}
	return reader
}

func (r *chunkReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	if err := r.contextErr(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.buffer) == 0 {
		if r.done {
			r.releaseBatch()
			return 0, io.EOF
		}
		if err := r.refill(); err != nil {
			var corruption *CorruptionError
			if errors.As(err, &corruption) {
				r.buffer = nil
				r.terminal = corruption
				r.metrics.corruptReads.Inc()
				return 0, r.terminal
			}
			return 0, err
		}
	}
	if err := r.contextErr(); err != nil {
		return 0, err
	}
	n := copy(p, r.buffer)
	r.buffer = r.buffer[n:]
	if len(r.buffer) == 0 && r.batchPos > 0 {
		r.batch[r.batchPos-1].Data = nil
	}
	return n, nil
}

func (r *chunkReader) refill() error {
	if err := r.contextErr(); err != nil {
		return err
	}
	if r.batchPos < len(r.batch) {
		r.activateChunk()
		return nil
	}
	r.releaseBatch()

	first := r.next
	count := min(uint64(r.prefetch), uint64(r.last)-uint64(first)+1)
	last := first + uint32(count-1)
	finishQuery := r.metrics.startOperation("chunk_query")
	chunks, err := r.store.Chunks(r.ctx, r.manifest.Key, r.manifest.Generation, first, last)
	finishQuery(err)
	r.metrics.chunkQueries.Inc()
	var prefetchedBytes int
	for i := range chunks {
		prefetchedBytes += len(chunks[i].Data)
	}
	r.metrics.readPrefetchBytes.Add(float64(prefetchedBytes))
	if err := r.contextErr(); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("read ClickHouse object %q generation %s chunks %d-%d: %w", r.manifest.Key, r.manifest.Generation, first, last, err)
	}
	if len(chunks) != int(count) {
		return corruptManifest(r.manifest, fmt.Sprintf("chunk request %d-%d returned %d chunks", first, last, len(chunks)))
	}
	for i := range chunks {
		expectedIndex := first + uint32(i)
		if chunks[i].Index != expectedIndex {
			return corruptManifest(r.manifest, fmt.Sprintf("chunk request %d-%d returned index %d at position %d", first, last, chunks[i].Index, i))
		}
		expectedLength := uint64(r.manifest.ChunkSize)
		if chunks[i].Index == r.manifest.ChunkCount-1 {
			expectedLength = r.manifest.Size - uint64(chunks[i].Index)*uint64(r.manifest.ChunkSize)
		}
		if uint64(len(chunks[i].Data)) != expectedLength {
			return corruptManifest(r.manifest, fmt.Sprintf("chunk %d length is %d, expected %d", chunks[i].Index, len(chunks[i].Data), expectedLength))
		}
	}

	r.batch = chunks
	r.next = last + 1
	r.activateChunk()
	return nil
}

func (r *chunkReader) activateChunk() {
	value := &r.batch[r.batchPos]
	r.batchPos++
	chunkStart := uint64(value.Index) * uint64(r.manifest.ChunkSize)
	from := max(r.start, chunkStart) - chunkStart
	to := min(r.end, chunkStart+uint64(len(value.Data))) - chunkStart
	r.buffer = value.Data[from:to]
	r.done = value.Index == r.last
	if r.batchPos == len(r.batch) && len(r.buffer) == 0 {
		r.releaseBatch()
	}
}

func (r *chunkReader) releaseBatch() {
	r.batch = nil
	r.batchPos = 0
}

func (r *chunkReader) contextErr() error {
	if err := r.ctx.Err(); err != nil {
		r.buffer = nil
		r.releaseBatch()
		return fmt.Errorf("read ClickHouse object %q chunks: %w", r.manifest.Key, err)
	}
	return nil
}

func (r *chunkReader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	r.buffer = nil
	r.releaseBatch()
	return nil
}

func (r *chunkReader) ObjectSize() (int64, error) {
	return int64(r.end - r.start), nil
}

var _ io.ReadCloser = (*chunkReader)(nil)
var _ objstore.ObjectSizer = (*chunkReader)(nil)
