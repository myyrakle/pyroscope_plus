package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestIterMatchesFilesystemDirectoryBehavior(t *testing.T) {
	store := &fakeStore{listResult: []manifest{
		{Key: "foo/bar/buz1", State: committed},
		{Key: "foo/bar/buz2", State: committed},
		{Key: "foo/ba/buzz3", State: committed},
		{Key: "foo/buzz4", State: committed},
		{Key: "foo/buzz5", State: committed},
		{Key: "foo6", State: committed},
	}}
	bucket := testBucket(bucketTestConfig(), store)

	tests := []struct {
		name string
		dir  string
		opts []objstore.IterOption
		want []string
	}{
		{name: "foo slash", dir: "foo/", want: []string{"foo/ba/", "foo/bar/", "foo/buzz4", "foo/buzz5"}},
		{name: "foo slash recursive", dir: "foo/", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: []string{"foo/ba/buzz3", "foo/bar/buz1", "foo/bar/buz2", "foo/buzz4", "foo/buzz5"}},
		{name: "foo ba recursive", dir: "foo/ba", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: []string{"foo/ba/buzz3"}},
		{name: "foo ba slash recursive", dir: "foo/ba/", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: []string{"foo/ba/buzz3"}},
		{name: "foo b", dir: "foo/b", want: nil},
		{name: "foo b recursive", dir: "foo/b", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: nil},
		{name: "foo", dir: "foo", want: []string{"foo/ba/", "foo/bar/", "foo/buzz4", "foo/buzz5"}},
		{name: "foo recursive", dir: "foo", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: []string{"foo/ba/buzz3", "foo/bar/buz1", "foo/bar/buz2", "foo/buzz4", "foo/buzz5"}},
		{name: "fo", dir: "fo", want: nil},
		{name: "fo recursive", dir: "fo", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: nil},
		{name: "root", want: []string{"foo/", "foo6"}},
		{name: "root recursive", opts: []objstore.IterOption{objstore.WithRecursiveIter()}, want: []string{"foo/ba/buzz3", "foo/bar/buz1", "foo/bar/buz2", "foo/buzz4", "foo/buzz5", "foo6"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			err := bucket.Iter(context.Background(), tt.dir, func(name string) error {
				got = append(got, name)
				return nil
			}, tt.opts...)

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	require.Equal(t, []string{"foo/", "foo/", "foo/ba/", "foo/ba/", "foo/b/", "foo/b/", "foo/", "foo/", "fo/", "fo/", "", ""}, store.lists)
}

func TestIterUsesLatestLiveSnapshot(t *testing.T) {
	store := &fakeStore{listResult: []manifest{
		{Key: "a", Generation: uuid.New(), State: committed, Version: 3},
		{Key: "deleted", Generation: uuid.New(), State: deleted, Version: 8},
		{Key: "directory/live", Generation: uuid.New(), State: committed, Version: 5},
		{Key: "pending", Generation: uuid.New(), State: pending, Version: 9},
	}}
	bucket := testBucket(bucketTestConfig(), store)

	var got []string
	require.NoError(t, bucket.Iter(context.Background(), "", func(name string) error {
		got = append(got, name)
		return nil
	}, objstore.WithRecursiveIter()))
	require.Equal(t, []string{"a", "directory/live"}, got)
	require.Equal(t, []string{""}, store.lists)
}

func TestIterResolvesOverwriteAndTombstoneFromManifestHistory(t *testing.T) {
	oldGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	newGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	deletedGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	oldCommitAt := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	newCommitAt := oldCommitAt.Add(time.Hour)
	leaseExpiresAt := newCommitAt.Add(time.Hour)

	conn := &fakeClickHouseConnection{selectFn: selectLatestManifestsFromHistory(t,
		manifest{Key: "deleted", Generation: oldGeneration, State: pending, Version: 1, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "deleted", Generation: oldGeneration, State: committed, Version: 1, EventAt: oldCommitAt},
		manifest{Key: "deleted", Generation: deletedGeneration, State: deleted, Version: 2, EventAt: newCommitAt},
		manifest{Key: "overwritten", Generation: oldGeneration, State: pending, Version: 3, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "overwritten", Generation: oldGeneration, State: committed, Version: 3, EventAt: oldCommitAt},
		manifest{Key: "overwritten", Generation: newGeneration, State: pending, Version: 4, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "overwritten", Generation: newGeneration, State: committed, Version: 4, EventAt: newCommitAt},
	)}
	bucket := testBucket(bucketTestConfig(), testClickHouseStore(t, conn))

	var got []objstore.IterObjectAttributes
	err := bucket.IterWithAttributes(context.Background(), "", func(attrs objstore.IterObjectAttributes) error {
		got = append(got, attrs)
		return nil
	}, objstore.WithRecursiveIter(), objstore.WithUpdatedAt())

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "overwritten", got[0].Name)
	lastModified, ok := got[0].LastModified()
	require.True(t, ok)
	require.Equal(t, newCommitAt, lastModified)
}

func TestIterCallbacksRunAfterListCompletes(t *testing.T) {
	all := pagedIterManifests(1001)
	completedQueries := 0
	store := &fakeStore{listFn: func(_ context.Context, _, afterKey string, limit int) ([]manifest, error) {
		defer func() { completedQueries++ }()
		if afterKey == "" {
			return all[:limit], nil
		}
		return all[limit:], nil
	}}

	var got []string
	err := testBucket(bucketTestConfig(), store).Iter(context.Background(), "", func(name string) error {
		got = append(got, name)
		require.Equal(t, len(got), completedQueries)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"dir/", "z"}, got)
	require.Equal(t, 2, completedQueries)
}

func TestIterUsesBoundedKeysetPages(t *testing.T) {
	store := &fakeStore{listResult: pagedIterManifests(1001)}
	bucket := testBucket(bucketTestConfig(), store)

	var got []string
	err := bucket.Iter(context.Background(), "", func(name string) error {
		got = append(got, name)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"dir/", "z"}, got)
	require.Equal(t, []listRequest{
		{prefix: "", afterKey: "", limit: 1000},
		{prefix: "", afterKey: "dir/0999", limit: 1000},
	}, store.listCalls)
}

func TestIterUsesBoundedKeysetPagesRecursively(t *testing.T) {
	store := &fakeStore{listResult: pagedIterManifests(1001)}
	bucket := testBucket(bucketTestConfig(), store)

	var got []string
	err := bucket.Iter(context.Background(), "", func(name string) error {
		got = append(got, name)
		return nil
	}, objstore.WithRecursiveIter())

	require.NoError(t, err)
	require.Len(t, got, 1002)
	require.Equal(t, "dir/0000", got[0])
	require.Equal(t, "dir/1000", got[1000])
	require.Equal(t, "z", got[1001])
	require.Len(t, store.listCalls, 2)
	for _, call := range store.listCalls {
		require.Equal(t, 1000, call.limit)
	}
}

func TestIterRejectsUnorderedStorePage(t *testing.T) {
	store := &fakeStore{listFn: func(context.Context, string, string, int) ([]manifest, error) {
		return []manifest{{Key: "b", State: committed}, {Key: "a", State: committed}}, nil
	}}
	called := false

	err := testBucket(bucketTestConfig(), store).Iter(context.Background(), "", func(string) error {
		called = true
		return nil
	})

	require.ErrorContains(t, err, "strict lexical order")
	require.False(t, called)
}

func TestIterRejectsOversizedStorePageBeforeCallbacks(t *testing.T) {
	store := &fakeStore{listFn: func(context.Context, string, string, int) ([]manifest, error) {
		return pagedIterManifests(1000), nil
	}}
	called := false

	err := testBucket(bucketTestConfig(), store).Iter(context.Background(), "", func(string) error {
		called = true
		return nil
	})

	require.ErrorContains(t, err, "page limit")
	require.False(t, called)
}

func TestIterStopsPagingAfterCallbackErrorOrCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		callback func(context.CancelFunc) error
		wantErr  error
	}{
		{name: "callback error", callback: func(context.CancelFunc) error { return errors.New("stop") }},
		{name: "cancellation", callback: func(cancel context.CancelFunc) error { cancel(); return nil }, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &fakeStore{listResult: pagedIterManifests(1001)}
			callbackErr := tc.callback

			err := testBucket(bucketTestConfig(), store).Iter(ctx, "", func(string) error {
				return callbackErr(cancel)
			}, objstore.WithRecursiveIter())

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.Error(t, err)
			}
			require.Len(t, store.listCalls, 1)
		})
	}
}

func pagedIterManifests(directoryObjects int) []manifest {
	result := make([]manifest, 0, directoryObjects+1)
	for i := range directoryObjects {
		result = append(result, manifest{Key: fmt.Sprintf("dir/%04d", i), State: committed})
	}
	return append(result, manifest{Key: "z", State: committed})
}

func TestIterPropagatesStoreAndCallbackErrors(t *testing.T) {
	storeErr := errors.New("list failed")
	callbackErr := errors.New("callback failed")

	t.Run("store", func(t *testing.T) {
		called := false
		err := testBucket(bucketTestConfig(), &fakeStore{listErr: storeErr}).Iter(context.Background(), "", func(string) error {
			called = true
			return nil
		})
		require.ErrorIs(t, err, storeErr)
		require.False(t, called)
	})

	t.Run("callback", func(t *testing.T) {
		store := &fakeStore{listResult: []manifest{{Key: "a", State: committed}, {Key: "b", State: committed}}}
		var got []string
		err := testBucket(bucketTestConfig(), store).Iter(context.Background(), "", func(name string) error {
			got = append(got, name)
			return callbackErr
		})
		require.ErrorIs(t, err, callbackErr)
		require.Equal(t, []string{"a"}, got)
	})
}

func TestIterPropagatesContextCancellation(t *testing.T) {
	t.Run("before query", func(t *testing.T) {
		store := &fakeStore{}
		err := testBucket(bucketTestConfig(), store).Iter(canceledContext(), "", func(string) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, store.lists)
	})

	t.Run("during query", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &fakeStore{listFn: func(context.Context, string, string, int) ([]manifest, error) {
			cancel()
			return nil, nil
		}}

		err := testBucket(bucketTestConfig(), store).Iter(ctx, "", func(string) error { return nil })

		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, store.listCalls, 1)
	})

	t.Run("during callbacks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &fakeStore{listResult: []manifest{{Key: "a", State: committed}, {Key: "b", State: committed}}}
		var got []string
		err := testBucket(bucketTestConfig(), store).Iter(ctx, "", func(name string) error {
			got = append(got, name)
			cancel()
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, []string{"a"}, got)
	})
}

func TestIterValidatesOptions(t *testing.T) {
	store := &fakeStore{}
	unsupported := objstore.IterOption{Type: objstore.IterOptionType(100), Apply: func(*objstore.IterParams) {}}

	err := testBucket(bucketTestConfig(), store).Iter(context.Background(), "", func(string) error { return nil }, unsupported)
	require.ErrorIs(t, err, objstore.ErrOptionNotSupported)
	require.Empty(t, store.lists)
}

func TestIterWithAttributesPopulatesLastModifiedOnlyWhenRequested(t *testing.T) {
	updatedAt := time.Date(2026, 7, 16, 4, 0, 0, 0, time.UTC)
	store := &fakeStore{listResult: []manifest{
		{Key: "dir/object", State: committed, EventAt: updatedAt},
		{Key: "root", State: committed, EventAt: updatedAt.Add(time.Minute)},
	}}
	bucket := testBucket(bucketTestConfig(), store)

	for _, tc := range []struct {
		name      string
		opts      []objstore.IterOption
		wantNames []string
		wantOK    []bool
	}{
		{name: "no options", wantNames: []string{"dir/", "root"}, wantOK: []bool{false, false}},
		{name: "updated at", opts: []objstore.IterOption{objstore.WithUpdatedAt()}, wantNames: []string{"dir/", "root"}, wantOK: []bool{false, true}},
		{name: "recursive updated at", opts: []objstore.IterOption{objstore.WithRecursiveIter(), objstore.WithUpdatedAt()}, wantNames: []string{"dir/object", "root"}, wantOK: []bool{true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []objstore.IterObjectAttributes
			require.NoError(t, bucket.IterWithAttributes(context.Background(), "", func(attrs objstore.IterObjectAttributes) error {
				got = append(got, attrs)
				return nil
			}, tc.opts...))
			require.Len(t, got, 2)
			require.Equal(t, tc.wantNames, []string{got[0].Name, got[1].Name})
			for i, attrs := range got {
				lastModified, ok := attrs.LastModified()
				require.Equal(t, tc.wantOK[i], ok)
				if ok {
					require.Equal(t, updatedAt.Add(time.Duration(i)*time.Minute), lastModified)
				}
			}
		})
	}
}
