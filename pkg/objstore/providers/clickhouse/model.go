package clickhouse

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type manifestState uint8

const (
	pending manifestState = iota + 1
	committed
	deleted
)

func (s manifestState) String() string {
	switch s {
	case pending:
		return "pending"
	case committed:
		return "committed"
	case deleted:
		return "deleted"
	default:
		return fmt.Sprintf("manifestState(%d)", s)
	}
}

type manifest struct {
	Key            string
	Generation     uuid.UUID
	Size           uint64
	ChunkCount     uint32
	ChunkSize      uint32
	State          manifestState
	Version        uint64
	LeaseExpiresAt time.Time
	EventAt        time.Time
}

type chunk struct {
	Key        string
	Generation uuid.UUID
	Index      uint32
	Data       []byte
	FullLength uint64
	CreatedAt  time.Time
}

type cleanupCandidate struct {
	Key        string
	Generation uuid.UUID
}

var ErrObjectNotFound = errors.New("ClickHouse object not found")

type CorruptionError struct {
	Key        string
	Generation uuid.UUID
	Reason     string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("ClickHouse object %q generation %s is corrupt: %s", e.Key, e.Generation, e.Reason)
}

type versionClock struct {
	now  func() time.Time
	last atomic.Uint64
}

func newVersionClock(now func() time.Time) *versionClock {
	return &versionClock{now: now}
}

func (c *versionClock) Next() (uint64, error) {
	nanos := c.now().UnixNano()
	if nanos < 0 {
		return 0, errors.New("ClickHouse version clock time is before Unix epoch")
	}
	wall := uint64(nanos) + 1
	for {
		last := c.last.Load()
		if last == ^uint64(0) {
			return 0, errors.New("ClickHouse version clock is exhausted")
		}
		next := wall
		if next <= last {
			next = last + 1
		}
		if c.last.CompareAndSwap(last, next) {
			return next - 1, nil
		}
	}
}
