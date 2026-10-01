// Package loadtest provides helpers shared by the load tests of the implementation modules.
package loadtest

import (
	"math/rand/v2"
	"slices"
	"sync"
)

// Recorder records message IDs per key (a consumer, a publisher, a topic...). It is safe for concurrent use.
type Recorder struct {
	lock sync.Mutex
	ids  map[string][]string
}

func NewRecorder() *Recorder {
	return &Recorder{ids: map[string][]string{}}
}

// Record appends the message ID to the IDs recorded for the key.
func (r *Recorder) Record(key, id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.ids[key] = append(r.ids[key], id)
}

// IDs returns a copy of the IDs recorded for the key, in recording order.
func (r *Recorder) IDs(key string) []string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return slices.Clone(r.ids[key])
}

// Count returns the number of IDs recorded for the key.
func (r *Recorder) Count(key string) int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return len(r.ids[key])
}

// Missing returns the IDs of expected that are not in actual.
func Missing(expected, actual []string) []string {
	seen := toSet(actual)
	missing := []string{}
	for _, id := range expected {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

// Duplicates returns the IDs of b that are also in a.
func Duplicates(a, b []string) []string {
	seen := toSet(a)
	duplicates := []string{}
	for _, id := range b {
		if seen[id] {
			duplicates = append(duplicates, id)
		}
	}
	return duplicates
}

// RandomInt returns a random int in [lower, higher].
func RandomInt(lower, higher int) int {
	return rand.IntN(higher-lower+1) + lower
}

func toSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}
