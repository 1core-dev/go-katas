// Package shardedmap provides a high-performance, thread-safe map with sharded locks.
// It is optimized for read-heavy workloads and avoids global mutex contention.
package shardedmap

import (
	"hash/fnv"
	"sync"
)

// ShardedMap is a thread-safe map with configurable shard count.
// Reads are optimized with RWMutex on each shard.
type ShardedMap[K comparable, V any] struct {
	shards []shard[K, V]
	count  uint64
}

type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// New creates a new ShardedMap with the specified number of shards.
// Panics if shardsCount is 0.
func New[K comparable, V any](shardsCount uint64) *ShardedMap[K, V] {
	if shardsCount == 0 {
		panic("shardsCount must be > 0")
	}

	sm := &ShardedMap[K, V]{
		shards: make([]shard[K, V], shardsCount),
		count:  shardsCount,
	}

	for i := range sm.shards {
		sm.shards[i].m = make(map[K]V)
	}

	return sm
}

// Get returns the value associated with the key and a boolean indicating existence.
func (sm *ShardedMap[K, V]) Get(key K) (V, bool) {
	shard := sm.shardFor(key)

	shard.mu.RLock()
	v, ok := shard.m[key]
	shard.mu.RUnlock()

	return v, ok
}

// Set inserts or updates the key with the given value.
func (sm *ShardedMap[K, V]) Set(key K, value V) {
	shard := sm.shardFor(key)

	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}

// Delete removes the key from the map.
func (sm *ShardedMap[K, V]) Delete(key K) {
	shard := sm.shardFor(key)

	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

// Keys returns a snapshot of all keys in the map.
// Safe to call concurrently with Set/Delete.
func (sm *ShardedMap[K, V]) Keys() []K {
	var keys []K

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

// shardFor calculates the shard corresponding to the given key.
func (sm *ShardedMap[K, V]) shardFor(key K) *shard[K, V] {
	hash := hashKey(key)
	return &sm.shards[hash%sm.count]
}

// hashKey computes a stable FNV-1a hash for supported key types.
func hashKey[K comparable](key K) uint64 {
	h := fnv.New64a()

	switch v := any(key).(type) {
	case string:
		h.Write([]byte(v))
	case int:
		var b [8]byte
		u := uint64(v)
		for i := range 8 {
			b[i] = byte(u >> (i * 8))
		}
		h.Write(b[:])
	default:
		panic("unsupported key type")
	}

	return h.Sum64()
}
