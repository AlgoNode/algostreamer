package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

type eternalFn func(ctx context.Context) error

func Backoff(ctx context.Context, fn eternalFn, timeout time.Duration, wait time.Duration, maxwait time.Duration) error {
	//Loop until Algoverse gets cancelled
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cctx, cancel := context.WithTimeout(ctx, timeout)
		err := fn(cctx)
		if err == nil { // Success
			cancel()
			return nil
		}
		cancel()
		fmt.Fprintf(os.Stderr, "Error: %s", err)

		//keep an eye on cancellation while backing off
		if wait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
			if maxwait > 0 {
				wait *= 2
				if wait > maxwait {
					wait = maxwait
				}
			}
		}
	}
}
