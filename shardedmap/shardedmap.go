package shardedmap

import (
	"hash/fnv"
	"sync"
)

type SharderMap[K comparable, V any] struct {
	shards []shard[K, V]
	count  uint64
}
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func New[K comparable, V any](shardsCount uint64) *SharderMap[K, V] {
	if shardsCount == 0 {
		panic("shardsCount must be > 0")
	}

	sm := SharderMap[K, V]{
		shards: make([]shard[K, V], shardsCount),
		count:  shardsCount,
	}

	for i := range sm.shards {
		sm.shards[i].m = make(map[K]V)
	}

	return &sm
}

func (sm *SharderMap[K, V]) Get(key K) (V, bool) {
	shard := sm.shardFor(key)

	shard.mu.RLock()
	v, ok := shard.m[key]
	shard.mu.RUnlock()

	return v, ok
}

func (sm *SharderMap[K, V]) Set(key K, value V) {
	shard := sm.shardFor(key)

	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}

func (sm *SharderMap[K, V]) Delete(key K) {
	shard := sm.shardFor(key)

	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

func (sm *SharderMap[K, V]) Keys() []K {
	keys := make([]K, 0)

	for i := range sm.shards {
		shard := &sm.shards[i]

		shard.mu.RLock()
		for k := range shard.m {
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}

	return keys
}

func (sm *SharderMap[K, V]) shardFor(key K) *shard[K, V] {
	hash := hashKey(key)
	return &sm.shards[hash%sm.count]
}

func hashKey[K comparable](key K) uint64 {
	h := fnv.New64a()

	switch v := any(key).(type) {
	case string:
		h.Write([]byte(v))
	case int:
		var b [8]byte
		u := uint(v)
		for i := range 8 {
			b[i] = byte(u >> (i * 8))
		}
		h.Write(b[:])
	default:
		panic("unsupported key type")
	}

	return h.Sum64()
}
