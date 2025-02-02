package errorsampling

import (
	"math/rand/v2"
	"sort"
)

// sampleFrom returns k random elements from population without replacement.
func sampleFrom[T any](population []T, k int) []T {
	n := len(population)
	idx := rand.Perm(n)

	// in-place sort
	sort.Ints(idx)

	sample := make([]T, k)
	for i := 0; i < k; i++ {
		sample[i] = population[idx[i]]
	}

	return sample
}

func sliceToSet(nums []int) map[int]struct{} {
	set := make(map[int]struct{}, len(nums))
	for _, n := range nums {
		set[n] = struct{}{}
	}
	return set
}
