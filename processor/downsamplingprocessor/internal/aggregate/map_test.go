package aggregate_test

import (
	"hash/maphash"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tosuke/opentelemetry-collector-components/processor/downsamplingprocessor/internal/aggregate"
)

type testKey struct {
	id string
}

var seed = maphash.MakeSeed()

func (k testKey) Hash() uint64 {
	return maphash.Comparable(seed, k.id)
}

func (k testKey) Equal(other testKey) bool {
	return k.id == other.id
}

func TestMap_SetAndGet(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key1 := testKey{id: "key1"}

	m.Set(key1, "value1")
	value, exists := m.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, "value1", value)
}

func TestMap_GetNonExistentKey(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key2 := testKey{id: "key2"}

	_, exists := m.Get(key2)
	assert.False(t, exists)
}

func TestMap_Update(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key1 := testKey{id: "key1"}

	m.Set(key1, "value1")
	m.Set(key1, "value1_updated")
	value, exists := m.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, "value1_updated", value)
}

func TestMap_Delete(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key1 := testKey{id: "key1"}

	m.Set(key1, "value1")
	m.Delete(key1)
	_, exists := m.Get(key1)
	assert.False(t, exists)
}

func TestMap_Len(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key1 := testKey{id: "key1"}
	key2 := testKey{id: "key2"}

	m.Set(key1, "value1")
	m.Set(key2, "value2")
	assert.Equal(t, 2, m.Len())
}

func TestMap_All(t *testing.T) {
	m := aggregate.NewMap[testKey, string]()
	key1 := testKey{id: "key1"}
	key2 := testKey{id: "key2"}

	m.Set(key1, "value1")
	m.Set(key2, "value2")

	assert.Equal(t,
		map[string]string{"key1": "value1", "key2": "value2"},
		maps.Collect(func(yield func(string, string) bool) {
			for k, v := range m.All() {
				if !yield(k.id, v) {
					return
				}
			}
		}),
	)
}
