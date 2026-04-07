package shardedmap_test

import (
	"sync"
	"testing"

	"github.com/1core-dev/go-katas/shardedmap"
)

func TestSharedMapRace(t *testing.T) {
	const (
		ops    = 1000
		shards = 8
	)
	m := shardedmap.New[int, int](shards)
	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range ops {
			m.Set(i, i)
		}
	})

	wg.Go(func() {
		for i := range ops {
			m.Get(i)
		}
	})

	wg.Go(func() {
		for i := range ops {
			m.Delete(i)
		}
	})

	wg.Wait()
}

func TestShardedMapKeys(t *testing.T) {
	const (
		ops    = 1000
		shards = 8
	)
	m := shardedmap.New[int, int](8)

	for i := range ops {
		m.Set(i, i)
	}

	if len(m.Keys()) != ops {
		t.Fatalf("expected %d keys, got %d", ops, len(m.Keys()))
	}

	for _, k := range m.Keys() {
		v, ok := m.Get(k)
		if !ok || v != k {
			t.Fatalf("key %d missing or wrong value %d", k, v)
		}
	}
}

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

func BenchmarkSharedMapMemory(b *testing.B) {
	const (
		ops    = 1000
		shards = 8
	)
	m := shardedmap.New[int, int](8)

	b.ResetTimer()
	for i := range ops {
		m.Set(i, i)
	}

	if len(m.Keys()) != ops {
		b.Fatalf("expected %d keys, got %d", len(m.Keys()), ops)
	}
}
