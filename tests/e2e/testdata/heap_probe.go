// Compiled into the benchmark binary by a Go overlay. A signal captures one
// live-heap profile while the target is deliberately holding the writer.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"syscall"
	"time"
)

func init() {
	path := os.Getenv("STRAND_RAM_PROFILE")
	if path == "" {
		return
	}
	req := make(chan os.Signal, 1)
	signal.Notify(req, syscall.SIGUSR1)
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		ticks := time.NewTicker(50 * time.Millisecond)
		defer ticks.Stop()
		var peak uint64
		for {
			select {
			case <-ticks.C:
				metrics.Read(s)
				peak = max(peak, s[0].Value.Uint64())
			case <-req:
				runtime.GC()
				metrics.Read(s)
				f, err := os.Create(path + ".tmp")
				if err == nil {
					err = pprof.WriteHeapProfile(f)
					if e := f.Close(); err == nil {
						err = e
					}
				}
				if err == nil {
					err = os.Rename(path+".tmp", path)
				}
				fmt.Fprintf(os.Stderr, "heap_sample peak_bytes=%d live_bytes=%d profile_error=%v\n", peak, s[0].Value.Uint64(), err)
			}
		}
	}()
}
