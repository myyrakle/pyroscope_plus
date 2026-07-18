package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	objstoreotel "github.com/thanos-io/objstore/tracing/opentelemetry"
	"go.opentelemetry.io/otel"

	phlareobj "github.com/grafana/pyroscope/v2/pkg/objstore"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/azure"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/clickhouse"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/cos"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/filesystem"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/gcs"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/s3"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/swift"
	phlarecontext "github.com/grafana/pyroscope/v2/pkg/pyroscope/context"
)

type clickHouseBucketFactory func(context.Context, clickhouse.Config, string, log.Logger, prometheus.Registerer) (objstore.Bucket, error)

// NewBucket creates a new bucket client based on the configured backend
func NewBucket(ctx context.Context, cfg Config, name string) (phlareobj.Bucket, error) {
	return newBucket(ctx, cfg, name, newClickHouseBucketClient)
}

func newClickHouseBucketClient(ctx context.Context, cfg clickhouse.Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	return clickhouse.NewBucketClient(ctx, cfg, name, logger, reg)
}

func newBucket(ctx context.Context, cfg Config, name string, newClickHouseBucket clickHouseBucketFactory) (phlareobj.Bucket, error) {
	var (
		backendClient objstore.Bucket
		err           error
	)
	logger := phlarecontext.Logger(ctx)
	reg := phlarecontext.Registry(ctx)
	prefixPath := cfg.getPrefix()

	switch cfg.Backend {
	case S3:
		backendClient, err = s3.NewBucketClient(cfg.S3, name, logger)
	case GCS:
		backendClient, err = gcs.NewBucketClient(ctx, cfg.GCS, name, logger)
	case Azure:
		backendClient, err = azure.NewBucketClient(cfg.Azure, name, logger)
	case Swift:
		backendClient, err = swift.NewBucketClient(cfg.Swift, name, logger)
	case COS:
		backendClient, err = cos.NewBucketClient(cfg.COS, name, logger)
	case ClickHouse:
		backendClient, err = newClickHouseBucket(ctx, cfg.ClickHouse, name, logger, reg)
	case Filesystem:
		// Filesystem is a special case, as it is not a remote storage backend
		// We want to use a fileReaderAt to read and seek from the filesystem
		// This means middlewares and instrumentation is not triggered for `ReaderAt` function
		middlewares := []func(objstore.Bucket) (objstore.Bucket, error){
			func(b objstore.Bucket) (objstore.Bucket, error) {
				return objstore.WrapWithMetrics(b, reg, name), nil
			},
			func(b objstore.Bucket) (objstore.Bucket, error) {
				return objstoreotel.WrapWithTraces(b, otel.Tracer("objstore")), nil
			},
		}
		fs, err := filesystem.NewBucket(cfg.Filesystem.Directory, append(middlewares, cfg.Middlewares...)...)
		if err != nil {
			return nil, err
		}
		if prefixPath == "" {
			return fs, nil
		}
		return phlareobj.NewPrefixedBucket(fs, prefixPath), nil
	default:
		return nil, ErrUnsupportedStorageBackend
	}

	if err != nil {
		return nil, err
	}

	// Wrap the client with any provided middleware
	for _, wrap := range cfg.Middlewares {
		wrappedClient, wrapErr := wrap(backendClient)
		if wrapErr != nil {
			if closeErr := backendClient.Close(); closeErr != nil {
				return nil, errors.Join(wrapErr, fmt.Errorf("close bucket after middleware failure: %w", closeErr))
			}
			return nil, wrapErr
		}
		backendClient = wrappedClient
	}
	bkt := phlareobj.NewBucket(objstoreotel.WrapWithTraces(objstore.WrapWithMetrics(backendClient, reg, name), otel.Tracer("objstore")))

	if prefixPath != "" {
		bkt = phlareobj.NewPrefixedBucket(bkt, prefixPath)
	}
	return bkt, nil
}
