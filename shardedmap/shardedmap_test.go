package shardedmap_test

import (
	"sync"
	"testing"

	"github.com/1core-dev/go-katas/shardedmap"
)

func BenchmarkSharedMapSet(b *testing.B) {
	tests := []struct {
		shards uint64
		name   string
	}{
		{1, "1_shard"},
		{64, "64_shards"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			m := shardedmap.New[int, int](tt.shards)

			b.ResetTimer()
			var wg sync.WaitGroup

			for g := range 8 {
				wg.Go(func() {
					for i := range b.N {
						key := g*b.N + i
						m.Set(key, i)
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestSharedMapRace(t *testing.T) {
	const n = 1000
	m := shardedmap.New[int, int](n)
	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range n {
			m.Set(i, i)
		}
	})

	wg.Go(func() {
		for i := range n {
			m.Get(i)
		}
	})

	wg.Go(func() {
		for i := range n {
			m.Delete(i)
		}
	})

	wg.Wait()
}
