// Copyright (C) 2022 AlgoNode Org.
//
// algostreamer is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// algostreamer is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with algostreamer.  If not, see <https://www.gnu.org/licenses/>.

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
