package main

import "context"

const (
	maxWorkloadAllocationKiB = 1024
	cpuCancelCheckInterval   = 4096
)

//go:noinline
func runCPU(iterations int) uint64 {
	checksum, _ := runCPUContext(context.Background(), iterations)
	return checksum
}

//go:noinline
func runCPUContext(ctx context.Context, iterations int) (uint64, error) {
	var value uint64 = 1469598103934665603
	for i := 0; i < iterations; i++ {
		if i%cpuCancelCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		value ^= uint64(i + 1)
		value *= 1099511628211
	}
	return value, nil
}

func runAllocation(kib int) []byte {
	if kib <= 0 {
		return nil
	}
	if kib > maxWorkloadAllocationKiB {
		kib = maxWorkloadAllocationKiB
	}

	data := make([]byte, kib*1024)
	for i := range data {
		data[i] = byte(i)
	}
	return data
}
