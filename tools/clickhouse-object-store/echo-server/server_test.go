package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerGETRoutes(t *testing.T) {
	tests := []struct {
		path       string
		wantStatus string
		wantRoute  string
		wantWork   int
	}{
		{path: "/health", wantStatus: "ok"},
		{path: "/fast", wantRoute: "fast", wantWork: 1_000_000},
		{path: "/slow", wantRoute: "slow", wantWork: 12_000_000},
		{path: "/alloc", wantRoute: "alloc", wantWork: 512 * 1024},
	}

	server := newServer()
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, tt.path, nil)

			server.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code)
			require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))

			var response map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			if tt.wantStatus != "" {
				require.Equal(t, tt.wantStatus, response["status"])
				return
			}
			require.Equal(t, tt.wantRoute, response["route"])
			if tt.path == "/alloc" {
				require.Equal(t, float64(tt.wantWork), response["bytes"])
				return
			}
			require.Equal(t, float64(tt.wantWork), response["iterations"])
			require.NotZero(t, response["checksum"])
		})
	}
}

func TestServerHTTPTimeouts(t *testing.T) {
	server := newServer()

	require.Equal(t, 5*time.Second, server.Server.ReadHeaderTimeout)
	require.Equal(t, 15*time.Second, server.Server.ReadTimeout)
	require.Equal(t, 30*time.Second, server.Server.WriteTimeout)
	require.Equal(t, 60*time.Second, server.Server.IdleTimeout)
}

func TestServerRejectsNonGETWorkloadRequests(t *testing.T) {
	server := newServer()
	for _, path := range []string{"/health", "/fast", "/slow", "/alloc"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, nil)

		server.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusMethodNotAllowed, recorder.Code, path)
	}
}

func TestServerLimitsConcurrentManualWorkloads(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	block := func(context.Context) {
		started <- struct{}{}
		<-release
	}
	server := newServerWithWorkloads(workloadSet{
		fast: func(ctx context.Context) (uint64, error) {
			block(ctx)
			return 1, nil
		},
		slow: func(ctx context.Context) (uint64, error) {
			block(ctx)
			return 2, nil
		},
		alloc: func(ctx context.Context) (int, error) {
			block(ctx)
			return 3, nil
		},
	})

	responses := make(chan int, 2)
	for _, path := range []string{"/fast", "/slow"} {
		path := path
		go func() {
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
			responses <- recorder.Code
		}()
	}
	receiveTestChannel(t, started, "first manual workload did not start")
	receiveTestChannel(t, started, "second manual workload did not start")

	overloaded := httptest.NewRecorder()
	server.ServeHTTP(overloaded, httptest.NewRequest(http.MethodGet, "/alloc", nil))
	require.Equal(t, http.StatusTooManyRequests, overloaded.Code)

	health := httptest.NewRecorder()
	server.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusOK, health.Code)

	close(release)
	require.Equal(t, http.StatusOK, receiveTestChannel(t, responses, "first manual workload did not finish"))
	require.Equal(t, http.StatusOK, receiveTestChannel(t, responses, "second manual workload did not finish"))

	afterRelease := httptest.NewRecorder()
	server.ServeHTTP(afterRelease, httptest.NewRequest(http.MethodGet, "/alloc", nil))
	require.Equal(t, http.StatusOK, afterRelease.Code)
	receiveTestChannel(t, started, "manual workload did not start after capacity was released")

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := httptest.NewRecorder()
	canceledRequest := httptest.NewRequest(http.MethodGet, "/fast", nil).WithContext(canceledCtx)
	server.ServeHTTP(canceled, canceledRequest)
	require.Equal(t, http.StatusRequestTimeout, canceled.Code)
	require.Zero(t, len(started))
}

func TestServerDoesNotWriteSuccessAfterWorkloadCancellation(t *testing.T) {
	started := make(chan struct{})
	server := newServerWithWorkloads(workloadSet{
		fast: func(ctx context.Context) (uint64, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		},
		slow:  func(context.Context) (uint64, error) { return 0, nil },
		alloc: func(context.Context) (int, error) { return 0, nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		request := httptest.NewRequest(http.MethodGet, "/fast", nil).WithContext(ctx)
		server.ServeHTTP(recorder, request)
		close(done)
	}()

	receiveTestChannel(t, started, "cancelable workload did not start")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled workload handler did not return")
	}
	require.Equal(t, http.StatusRequestTimeout, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "checksum")
}

func TestRunAutomaticWorkloadExitsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runAutomaticWorkload(ctx, make(chan time.Time))
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("automatic workload did not exit after context cancellation")
	}
}

func TestAutomaticTickRunsEveryWorkloadOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ticks := make(chan time.Time)
	done := make(chan struct{})
	tickDone := make(chan struct{})
	var fastCount atomic.Int32
	var slowCount atomic.Int32
	var allocCount atomic.Int32
	workloads := workloadSet{
		fast: func(context.Context) (uint64, error) {
			fastCount.Add(1)
			return 1, nil
		},
		slow: func(context.Context) (uint64, error) {
			slowCount.Add(1)
			return 2, nil
		},
		alloc: func(context.Context) (int, error) {
			allocCount.Add(1)
			close(tickDone)
			return 3, nil
		},
	}
	go func() {
		runAutomaticWorkloadWith(ctx, ticks, workloads)
		close(done)
	}()

	sendTestChannel(t, ticks, time.Now(), "automatic workload did not accept a tick")
	select {
	case <-tickDone:
	case <-time.After(time.Second):
		t.Fatal("automatic workload did not finish a tick")
	}
	require.EqualValues(t, 1, fastCount.Load())
	require.EqualValues(t, 1, slowCount.Load())
	require.EqualValues(t, 1, allocCount.Load())

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("automatic workload did not stop")
	}
}

func TestAutomaticWorkloadStopsWhenActiveWorkIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	started := make(chan struct{})
	done := make(chan struct{})
	var slowCount atomic.Int32
	var allocCount atomic.Int32
	workloads := workloadSet{
		fast: func(ctx context.Context) (uint64, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		},
		slow: func(context.Context) (uint64, error) {
			slowCount.Add(1)
			return 0, nil
		},
		alloc: func(context.Context) (int, error) {
			allocCount.Add(1)
			return 0, nil
		},
	}
	go func() {
		runAutomaticWorkloadWith(ctx, ticks, workloads)
		close(done)
	}()

	sendTestChannel(t, ticks, time.Now(), "automatic workload did not accept a tick")
	receiveTestChannel(t, started, "automatic workload did not start active work")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("automatic workload did not stop active work after cancellation")
	}
	require.Zero(t, slowCount.Load())
	require.Zero(t, allocCount.Load())
}

func receiveTestChannel[T any](t *testing.T, channel <-chan T, timeoutMessage string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(time.Second):
		t.Fatal(timeoutMessage)
		var zero T
		return zero
	}
}

func sendTestChannel[T any](t *testing.T, channel chan<- T, value T, timeoutMessage string) {
	t.Helper()
	select {
	case channel <- value:
	case <-time.After(time.Second):
		t.Fatal(timeoutMessage)
	}
}
