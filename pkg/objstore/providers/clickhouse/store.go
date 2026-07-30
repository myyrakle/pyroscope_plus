package clickhouse

import (
	"context"
	"time"
)

// store is the persistence boundary used by the ClickHouse bucket implementation.
type store interface {
	Init(ctx context.Context) error
	BeginUpload(ctx context.Context, pending manifest) (manifest, error)
	InsertChunks(ctx context.Context, chunks []chunk) error
	CommitUpload(ctx context.Context, upload manifest) error
	InsertDelete(ctx context.Context, tombstone manifest) (manifest, error)
	LatestManifest(ctx context.Context, key string) (manifest, error)
	Chunks(ctx context.Context, object manifest, first, last uint32, start, end uint64) ([]chunk, error)
	ListLatest(ctx context.Context, prefix, afterKey string, limit int) ([]manifest, error)
	CleanupCandidates(ctx context.Context, partition uint32, grace time.Duration, limit int) ([]cleanupCandidate, error)
	DeleteGenerations(ctx context.Context, candidates []cleanupCandidate) error
	Close() error
}
