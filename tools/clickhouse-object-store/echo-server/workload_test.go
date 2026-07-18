package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelOnSecondCheckContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
}

func (c *cancelOnSecondCheckContext) Err() error {
	if c.checks.Add(1) == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestRunCPUIsDeterministic(t *testing.T) {
	first := runCPU(1_000)
	require.Equal(t, first, runCPU(1_000))
	require.NotEqual(t, first, runCPU(10_000))
}

func TestRunCPUContextStopsAtCancellationCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	testCtx := &cancelOnSecondCheckContext{Context: ctx, cancel: cancel}

	checksum, err := runCPUContext(testCtx, 12_000_000)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, checksum)
	require.EqualValues(t, 2, testCtx.checks.Load())
}

func TestRunAllocationIsBounded(t *testing.T) {
	require.Len(t, runAllocation(64), 64*1024)
}

func TestRunAllocationRejectsNegativeSize(t *testing.T) {
	require.Nil(t, runAllocation(-1))
}

func TestRunAllocationRejectsZeroSize(t *testing.T) {
	require.Nil(t, runAllocation(0))
}

func TestRunAllocationAllowsMaximumSize(t *testing.T) {
	require.Len(t, runAllocation(maxWorkloadAllocationKiB), maxWorkloadAllocationKiB*1024)
}

func TestRunAllocationClampsAboveMaximumSize(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	require.Len(t, runAllocation(maxInt), maxWorkloadAllocationKiB*1024)
}
