// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/clickhouse"
	"github.com/grafana/pyroscope/v2/pkg/objstore/providers/filesystem"
	phlarecontext "github.com/grafana/pyroscope/v2/pkg/pyroscope/context"
)

const (
	configWithS3Backend = `
backend: s3
s3:
  endpoint:          localhost
  bucket_name:       test
  access_key_id:     xxx
  secret_access_key: yyy
  insecure:          true
`

	configWithGCSBackend = `
backend: gcs
gcs:
  bucket_name:     test
  service_account: |-
    {
      "type": "service_account",
      "project_id": "id",
      "private_key_id": "id",
      "private_key": "-----BEGIN PRIVATE KEY-----\nSOMETHING\n-----END PRIVATE KEY-----\n",
      "client_email": "test@test.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.com"
    }
`

	configWithUnknownBackend = `
backend: unknown
`
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config      string
		expectedErr error
	}{
		"should create an S3 bucket": {
			config:      configWithS3Backend,
			expectedErr: nil,
		},
		"should create a GCS bucket": {
			config:      configWithGCSBackend,
			expectedErr: nil,
		},
		"should return error on unknown backend": {
			config:      configWithUnknownBackend,
			expectedErr: ErrUnsupportedStorageBackend,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Load config
			cfg := Config{}
			flagext.DefaultValues(&cfg)

			err := yaml.Unmarshal([]byte(testData.config), &cfg)
			require.NoError(t, err)

			// Instance a new bucket client from the config
			bucketClient, err := NewBucket(context.Background(), cfg, "test")
			require.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				require.NotNil(t, bucketClient)
				bucketClient.Close()
			} else {
				assert.Equal(t, nil, bucketClient)
			}
		})
	}
}

func TestClient_ConfigValidation(t *testing.T) {
	testCases := []struct {
		name          string
		cfg           Config
		expectedError error
		expectedLog   string
	}{
		{
			name: "prefix/valid",
			cfg:  Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "helloWORLD123"},
		},
		{
			name: "prefix/valid-subdir",
			cfg:  Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "hello/world/env"},
		},
		{
			name: "prefix/valid-subdir-trailing-slash",
			cfg:  Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "hello/world/env/"},
		},
		{
			name:          "prefix/invalid-directory-up",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: ".."},
			expectedError: ErrStoragePrefixInvalidCharacters,
		},
		{
			name:          "prefix/invalid-directory",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "."},
			expectedError: ErrStoragePrefixInvalidCharacters,
		},
		{
			name:          "prefix/invalid-absolute-path",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "/hello/world"},
			expectedError: ErrStoragePrefixStartsWithSlash,
		},
		{
			name:          "prefix/invalid-..-in-a-path-segement",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "hello/../test"},
			expectedError: ErrStoragePrefixInvalidCharacters,
		},
		{
			name:          "prefix/invalid-empty-path-segement",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "hello//test"},
			expectedError: ErrStoragePrefixEmptyPathSegment,
		},
		{
			name:          "prefix/invalid-emoji",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "👋"},
			expectedError: ErrStoragePrefixInvalidCharacters,
		},
		{
			name:          "prefix/invalid-exclamation-mark",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, Prefix: "hello!world"},
			expectedError: ErrStoragePrefixInvalidCharacters,
		},
		{
			name:          "unsupported backend",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: "flash drive"}},
			expectedError: ErrUnsupportedStorageBackend,
		},
		{
			name:        "prefix/valid-legacy-subdir-trailing-slash",
			cfg:         Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, DeprecatedStoragePrefix: "hello/world/env/"},
			expectedLog: "config has a deprecated storage.storage-prefix flag set",
		},
		{
			name:          "prefix/deprecated-invalid-exclamation-mark",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, DeprecatedStoragePrefix: "hello!world"},
			expectedError: ErrStoragePrefixInvalidCharacters,
			expectedLog:   "config has a deprecated storage.storage-prefix flag set",
		},
		{
			name:          "prefix/invalid-both-configs",
			cfg:           Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}, DeprecatedStoragePrefix: "hello-world1", Prefix: "hello-world2"},
			expectedError: ErrStoragePrefixBothFlagsSet,
		},
	}

	logBuf := new(bytes.Buffer)
	logger := log.NewLogfmtLogger(logBuf)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Reset log buffer
			logBuf.Reset()

			actualErr := tc.cfg.Validate(logger)
			if tc.expectedError != nil {
				assert.Equal(t, actualErr, tc.expectedError)
			} else {
				assert.NoError(t, actualErr)
			}
			if tc.expectedLog != "" {
				assert.Contains(t, logBuf.String(), tc.expectedLog)
			} else {
				assert.Empty(t, logBuf.String())
			}
		})
	}
}

func TestNewPrefixedBucketClient(t *testing.T) {
	t.Run("with prefix", func(t *testing.T) {
		ctx := context.Background()
		tempDir := t.TempDir()
		cfg := Config{
			StorageBackendConfig: StorageBackendConfig{
				Backend: Filesystem,
				Filesystem: filesystem.Config{
					Directory: tempDir,
				},
			},
			Prefix: "prefix",
		}

		client, err := NewBucket(ctx, cfg, "test")
		require.NoError(t, err)

		err = client.Upload(ctx, "file", bytes.NewBufferString("content"))
		assert.NoError(t, err)

		_, err = client.Get(ctx, "file")
		assert.NoError(t, err)

		filePath := path.Join(tempDir, "prefix", "file")
		assert.FileExists(t, filePath)

		b, err := os.ReadFile(filePath)
		assert.NoError(t, err)
		assert.Equal(t, "content", string(b))
	})

	t.Run("without prefix", func(t *testing.T) {
		ctx := context.Background()
		tempDir := t.TempDir()
		cfg := Config{
			StorageBackendConfig: StorageBackendConfig{
				Backend: Filesystem,
				Filesystem: filesystem.Config{
					Directory: tempDir,
				},
			},
		}

		client, err := NewBucket(ctx, cfg, "test")
		require.NoError(t, err)
		err = client.Upload(ctx, "file", bytes.NewBufferString("content"))
		require.NoError(t, err)

		_, err = client.Get(ctx, "file")
		assert.NoError(t, err)

		filePath := path.Join(tempDir, "file")
		assert.FileExists(t, filePath)

		b, err := os.ReadFile(filePath)
		assert.NoError(t, err)
		assert.Equal(t, "content", string(b))
	})
}

func TestClickHouseConfigDefaults(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)

	require.Contains(t, SupportedBackends, ClickHouse)
	require.Equal(t, Filesystem, cfg.Backend)
	require.True(t, cfg.ClickHouse.AutoCreateTables)
	require.False(t, cfg.ClickHouse.Cleanup.Enabled)
	require.NoError(t, cfg.ClickHouse.Validate())
}

func TestClickHouseConfigFlagsCanDisableDefaults(t *testing.T) {
	var cfg Config
	fs := flag.NewFlagSet("storage", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("storage.", fs)

	err := fs.Parse([]string{
		"-storage.backend=clickhouse",
		"-storage.clickhouse.auto-create-tables=false",
		"-storage.clickhouse.cleanup.enabled=false",
	})
	require.NoError(t, err)
	require.Equal(t, ClickHouse, cfg.Backend)
	require.False(t, cfg.ClickHouse.AutoCreateTables)
	require.False(t, cfg.ClickHouse.Cleanup.Enabled)
}

func TestClickHouseConfigYAMLAndSecretRedaction(t *testing.T) {
	const password = "not-for-output"
	var cfg Config
	flagext.DefaultValues(&cfg)

	err := yaml.Unmarshal([]byte(`
backend: clickhouse
clickhouse:
  addresses: one:9000
  database: profiles
  objects_table: object_manifests
  chunks_table: object_chunks
  username: pyroscope
  password: `+password+`
  auto_create_tables: false
  cleanup:
    enabled: false
`), &cfg)
	require.NoError(t, err)
	require.Equal(t, ClickHouse, cfg.Backend)
	require.Equal(t, flagext.StringSliceCSV{"one:9000"}, cfg.ClickHouse.Addresses)
	require.Equal(t, "profiles", cfg.ClickHouse.Database)
	require.Equal(t, "object_manifests", cfg.ClickHouse.ObjectsTable)
	require.Equal(t, "object_chunks", cfg.ClickHouse.ChunksTable)
	require.Equal(t, "pyroscope", cfg.ClickHouse.Username)
	require.Equal(t, password, cfg.ClickHouse.Password.String())
	require.False(t, cfg.ClickHouse.AutoCreateTables)
	require.False(t, cfg.ClickHouse.Cleanup.Enabled)
	require.NoError(t, cfg.Validate(log.NewNopLogger()))

	encoded, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), password)
	require.Contains(t, string(encoded), "********")
}

func TestClickHouseConfigValidationDelegatesToProvider(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)
	cfg.Backend = ClickHouse
	cfg.ClickHouse.Database = "invalid-database"

	err := cfg.Validate(log.NewNopLogger())
	require.ErrorContains(t, err, "invalid ClickHouse database")
}

func TestNewClickHouseBucketDispatchesAndUsesRemoteWrappers(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)
	cfg.Backend = ClickHouse
	cfg.Prefix = "tenant-a"

	reg := prometheus.NewRegistry()
	ctx := phlarecontext.WithRegistry(context.Background(), reg)
	underlying := objstore.NewInMemBucket()
	middlewareCalled := false
	cfg.Middlewares = []func(objstore.Bucket) (objstore.Bucket, error){
		func(bucket objstore.Bucket) (objstore.Bucket, error) {
			middlewareCalled = true
			require.Same(t, underlying, bucket)
			return bucket, nil
		},
	}

	client, err := newBucket(ctx, cfg, "clickhouse-test", func(
		_ context.Context,
		gotCfg clickhouse.Config,
		name string,
		_ log.Logger,
		gotReg prometheus.Registerer,
	) (objstore.Bucket, error) {
		require.Equal(t, cfg.ClickHouse, gotCfg)
		require.Equal(t, "clickhouse-test", name)
		require.Same(t, reg, gotReg)
		return underlying, nil
	})
	require.NoError(t, err)
	require.True(t, middlewareCalled)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	require.NoError(t, client.Upload(ctx, "binary", bytes.NewReader([]byte{0, 1, 2, 0xff})))
	require.Equal(t, []byte{0, 1, 2, 0xff}, underlying.Objects()["tenant-a/binary"])

	readerAt, err := client.ReaderAt(ctx, "binary")
	require.NoError(t, err)
	require.NoError(t, readerAt.Close())

	families, err := reg.Gather()
	require.NoError(t, err)
	require.True(t, hasMetricFamily(families, "objstore_bucket_operations_total"))
}

func TestNewClickHouseBucketClosesBackendOnceWhenMiddlewareFails(t *testing.T) {
	middlewareErr := errors.New("middleware failed")
	closeErr := errors.New("close failed")

	for _, tt := range []struct {
		name     string
		closeErr error
	}{
		{name: "close succeeds"},
		{name: "close fails", closeErr: closeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)
			cfg.Backend = ClickHouse
			cfg.Middlewares = []func(objstore.Bucket) (objstore.Bucket, error){
				func(objstore.Bucket) (objstore.Bucket, error) {
					return nil, middlewareErr
				},
			}
			backend := &closeCountingBucket{
				Bucket:   objstore.NewInMemBucket(),
				closeErr: tt.closeErr,
			}

			client, err := newBucket(context.Background(), cfg, "clickhouse-test", func(
				context.Context,
				clickhouse.Config,
				string,
				log.Logger,
				prometheus.Registerer,
			) (objstore.Bucket, error) {
				return backend, nil
			})

			require.Nil(t, client)
			require.ErrorIs(t, err, middlewareErr)
			if tt.closeErr != nil {
				require.ErrorIs(t, err, tt.closeErr)
			} else {
				require.NotErrorIs(t, err, closeErr)
			}
			require.Equal(t, int32(1), backend.closeCalls.Load())
		})
	}
}

type closeCountingBucket struct {
	objstore.Bucket
	closeCalls atomic.Int32
	closeErr   error
}

func (b *closeCountingBucket) Close() error {
	b.closeCalls.Add(1)
	return b.closeErr
}

func hasMetricFamily(families []*dto.MetricFamily, name string) bool {
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}
