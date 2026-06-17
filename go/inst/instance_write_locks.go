package inst

import (
	"sort"
	"sync"
)

// instanceWriteLockSet retains locks only while operations use or wait for them.
// The registry mutex protects bookkeeping, never backend I/O.
type instanceWriteLockSet struct {
	mutex sync.Mutex
	locks map[string]*instanceWriteLock
}

type instanceWriteLock struct {
	mutex sync.RWMutex
	users int
}

var instanceWriteLocks = instanceWriteLockSet{locks: make(map[string]*instanceWriteLock)}

func clusterWriteLockKey(clusterName string) string {
	return "cluster:" + clusterName
}

func instanceWriteLockKey(key *InstanceKey) string {
	return "instance:" + key.StringCode()
}

// acquire pins every lock before waiting and acquires distinct keys in a stable
// order, so overlapping multi-instance operations cannot deadlock.
func (set *instanceWriteLockSet) acquire(keys []string, exclusive bool) func() {
	unique := make(map[string]bool, len(keys))
	ordered := make([]string, 0, len(keys))
	for _, key := range keys {
		if !unique[key] {
			unique[key] = true
			ordered = append(ordered, key)
		}
	}
	sort.Strings(ordered)
	locks := make([]*instanceWriteLock, len(ordered))
	set.mutex.Lock()
	for i, key := range ordered {
		lock := set.locks[key]
		if lock == nil {
			lock = &instanceWriteLock{}
			set.locks[key] = lock
		}
		lock.users++
		locks[i] = lock
	}
	set.mutex.Unlock()
	for _, lock := range locks {
		if exclusive {
			lock.mutex.Lock()
		} else {
			lock.mutex.RLock()
		}
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			if exclusive {
				locks[i].mutex.Unlock()
			} else {
				locks[i].mutex.RUnlock()
			}
		}
		set.mutex.Lock()
		for i, key := range ordered {
			locks[i].users--
			if locks[i].users == 0 {
				delete(set.locks, key)
			}
		}
		set.mutex.Unlock()
	}
}
