package config

import (
	"sync"
	"testing"
)

func TestSpecIsThreadSafe(t *testing.T) {
	t.Parallel()

	// use a loop to increase the chance of reproducing the data race
	for i := 0; i < 100; i++ {
		var wg sync.WaitGroup
		goroutines := 2
		wg.Add(goroutines)
		for j := 0; j < goroutines; j++ {
			go func() {
				defer wg.Done()
				newSpec := Spec()
				newSpec.SetDefault(false, "http", "enabled")
			}()
		}

		wg.Wait()
	}
}
