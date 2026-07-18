package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/thanos-io/objstore"
)

const iterPageSize = 1000

// SupportedIterOptions returns the iteration options implemented by ClickHouse.
func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

// Iter calls f for each object or directory entry below dir in lexical order.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.iter(ctx, dir, func(attrs objstore.IterObjectAttributes) error {
		return f(attrs.Name)
	}, options...)
}

// IterWithAttributes calls f for each object or directory entry below dir in lexical order.
func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return b.iter(ctx, dir, f, options...)
}

func (b *Bucket) iter(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) (err error) {
	finish := b.metrics.startOperation("iter")
	defer func() { finish(err) }()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	params := objstore.ApplyIterOptions(options...)
	prefix := normalizeIterDir(dir)
	var afterKey, lastEmitted string
	emitted := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		manifests, err := b.store.ListLatest(ctx, prefix, afterKey, iterPageSize)
		if err != nil {
			return fmt.Errorf("iterate ClickHouse objects under %q after %q: %w", prefix, afterKey, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(manifests) > iterPageSize {
			return fmt.Errorf("iterate ClickHouse objects under %q: store returned %d rows, page limit is %d", prefix, len(manifests), iterPageSize)
		}

		previousKey := afterKey
		for _, value := range manifests {
			if value.Key <= previousKey {
				return fmt.Errorf("iterate ClickHouse objects under %q: raw keys are not in strict lexical order after %q", prefix, previousKey)
			}
			if !strings.HasPrefix(value.Key, prefix) {
				return fmt.Errorf("iterate ClickHouse objects under %q: store returned key %q outside prefix", prefix, value.Key)
			}
			previousKey = value.Key
		}

		for _, value := range manifests {
			if value.State != committed {
				continue
			}
			name := value.Key
			isPrefix := false
			if !params.Recursive {
				if slash := strings.IndexByte(strings.TrimPrefix(value.Key, prefix), objstore.DirDelim[0]); slash >= 0 {
					name = value.Key[:len(prefix)+slash+1]
					isPrefix = true
				}
			}
			if emitted && name == lastEmitted {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			attrs := objstore.IterObjectAttributes{Name: name}
			if params.LastModified && !isPrefix {
				attrs.SetLastModified(value.EventAt)
			}
			if err := f(attrs); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			lastEmitted = name
			emitted = true
		}

		if len(manifests) < iterPageSize {
			return nil
		}
		afterKey = manifests[len(manifests)-1].Key
	}
}

func normalizeIterDir(dir string) string {
	if dir == "" || strings.HasSuffix(dir, objstore.DirDelim) {
		return dir
	}
	return dir + objstore.DirDelim
}
