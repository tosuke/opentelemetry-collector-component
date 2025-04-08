package aggregate

import (
	"iter"
)

type Key[K any] interface {
	Hash() uint64
	Equal(other K) bool
}

type entry[K Key[K], V any] struct {
	key   K
	value V
}

type Map[K Key[K], V any] struct {
	m map[uint64][]entry[K, V]
}

func NewMap[K Key[K], V any]() Map[K, V] {
	return Map[K, V]{m: make(map[uint64][]entry[K, V])}
}

func CollectMap[K Key[K], V any](seq iter.Seq2[K, V]) Map[K, V] {
	m := NewMap[K, V]()
	for key, value := range seq {
		m.Set(key, value)
	}
	return m
}

func (m Map[K, V]) Get(k K) (V, bool) {
	var zeroValue V
	hash := k.Hash()
	values, exists := m.m[hash]
	if !exists {
		return zeroValue, false
	}
	for _, entry := range values {
		if entry.key.Equal(k) {
			return entry.value, true
		}
	}
	return zeroValue, false
}

func (m Map[K, V]) Set(k K, v V) {
	hash := k.Hash()
	values, exists := m.m[hash]
	if !exists {
		values = []entry[K, V]{}
	}
	for i, entry := range values {
		if entry.key.Equal(k) {
			values[i].value = v
			return
		}
	}
	m.m[hash] = append(values, entry[K, V]{key: k, value: v})
}

func (m Map[K, V]) Delete(k K) {
	hash := k.Hash()
	values, exists := m.m[hash]
	if !exists {
		return
	}
	for i, entry := range values {
		if entry.key.Equal(k) {
			values = append(values[:i], values[i+1:]...)
			break
		}
	}
	if len(values) == 0 {
		delete(m.m, hash)
	} else {
		m.m[hash] = values
	}
}

func (m Map[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, values := range m.m {
			for _, entry := range values {
				if !yield(entry.value) {
					return
				}
			}
		}
	}
}

func (m Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, values := range m.m {
			for _, entry := range values {
				if !yield(entry.key, entry.value) {
					return
				}
			}
		}
	}
}

func (m Map[K, V]) Len() int {
	count := 0
	for _, values := range m.m {
		count += len(values)
	}
	return count
}
