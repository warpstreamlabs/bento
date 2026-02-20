package config

import (
	"sync"
	"testing"
)

func TestSpecIsThreadSafe(t *testing.T) {
	t.Parallel()

	// use a loop to increase the chance of reproducing the data race
	for range 100 {
		var wg sync.WaitGroup
		goroutines := 2
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				newSpec := Spec()
				newSpec.SetDefault(false, "http", "enabled")
			}()
		}

		wg.Wait()
	}
}
