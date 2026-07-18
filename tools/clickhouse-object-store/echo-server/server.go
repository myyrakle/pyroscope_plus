package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/grafana/pyroscope-go"
	"github.com/labstack/echo/v4"
)

const (
	fastIterations              = 1_000_000
	slowIterations              = 12_000_000
	allocationKiB               = 512
	maxConcurrentManualWorkload = 2
)

type workloadSet struct {
	fast  func(context.Context) (uint64, error)
	slow  func(context.Context) (uint64, error)
	alloc func(context.Context) (int, error)
}

func newServer() *echo.Echo {
	return newServerWithWorkloads(workloadSet{
		fast: func(ctx context.Context) (uint64, error) {
			return runProfiledCPU(ctx, "fast", "fast", fastIterations)
		},
		slow: func(ctx context.Context) (uint64, error) {
			return runProfiledCPU(ctx, "slow", "slow", slowIterations)
		},
		alloc: func(ctx context.Context) (int, error) {
			return runProfiledAllocation(ctx, "alloc", "alloc", allocationKiB)
		},
	})
}

func newServerWithWorkloads(workloads workloadSet) *echo.Echo {
	server := echo.New()
	server.HideBanner = true
	server.HidePort = true
	server.Server.ReadHeaderTimeout = 5 * time.Second
	server.Server.ReadTimeout = 15 * time.Second
	server.Server.WriteTimeout = 30 * time.Second
	server.Server.IdleTimeout = 60 * time.Second

	server.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	admission := workloadAdmission(make(chan struct{}, maxConcurrentManualWorkload))
	server.GET("/fast", func(c echo.Context) error {
		checksum, err := workloads.fast(c.Request().Context())
		if err != nil {
			return workloadErrorResponse(c, err)
		}
		return c.JSON(http.StatusOK, map[string]any{
			"route":      "fast",
			"iterations": fastIterations,
			"checksum":   checksum,
		})
	}, admission)
	server.GET("/slow", func(c echo.Context) error {
		checksum, err := workloads.slow(c.Request().Context())
		if err != nil {
			return workloadErrorResponse(c, err)
		}
		return c.JSON(http.StatusOK, map[string]any{
			"route":      "slow",
			"iterations": slowIterations,
			"checksum":   checksum,
		})
	}, admission)
	server.GET("/alloc", func(c echo.Context) error {
		allocatedBytes, err := workloads.alloc(c.Request().Context())
		if err != nil {
			return workloadErrorResponse(c, err)
		}
		return c.JSON(http.StatusOK, map[string]any{
			"route": "alloc",
			"bytes": allocatedBytes,
		})
	}, admission)

	return server
}

func workloadErrorResponse(c echo.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return c.JSON(http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
	}
	return c.JSON(http.StatusInternalServerError, map[string]string{"error": "workload failed"})
}

func workloadAdmission(slots chan struct{}) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ctx := c.Request().Context()
			if ctx.Err() != nil {
				return c.JSON(http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
				return next(c)
			case <-ctx.Done():
				return c.JSON(http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
			default:
				return c.JSON(http.StatusTooManyRequests, map[string]string{"error": "too many concurrent workloads"})
			}
		}
	}
}

func runProfiledCPU(ctx context.Context, route, function string, iterations int) (uint64, error) {
	var checksum uint64
	var err error
	pyroscope.TagWrapper(ctx, pyroscope.Labels("route", route, "function", function), func(taggedCtx context.Context) {
		checksum, err = runCPUContext(taggedCtx, iterations)
	})
	return checksum, err
}

func runProfiledAllocation(ctx context.Context, route, function string, kib int) (int, error) {
	var allocatedBytes int
	var err error
	pyroscope.TagWrapper(ctx, pyroscope.Labels("route", route, "function", function), func(taggedCtx context.Context) {
		if err = taggedCtx.Err(); err != nil {
			return
		}
		allocatedBytes = len(runAllocation(kib))
	})
	return allocatedBytes, err
}

func automaticWorkloads() workloadSet {
	return workloadSet{
		fast: func(ctx context.Context) (uint64, error) {
			return runProfiledCPU(ctx, "automatic", "fast", fastIterations)
		},
		slow: func(ctx context.Context) (uint64, error) {
			return runProfiledCPU(ctx, "automatic", "slow", slowIterations)
		},
		alloc: func(ctx context.Context) (int, error) {
			return runProfiledAllocation(ctx, "automatic", "alloc", allocationKiB)
		},
	}
}

func runAutomaticWorkload(ctx context.Context, ticks <-chan time.Time) {
	runAutomaticWorkloadWith(ctx, ticks, automaticWorkloads())
}

func runAutomaticWorkloadWith(ctx context.Context, ticks <-chan time.Time, workloads workloadSet) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			if _, err := workloads.fast(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if _, err := workloads.slow(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if _, err := workloads.alloc(ctx); err != nil && ctx.Err() != nil {
				return
			}
		}
	}
}
