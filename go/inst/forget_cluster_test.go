package inst

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/openark/orchestrator/go/config"
	"github.com/openark/orchestrator/go/db"
)

func setupForgetBackend(t *testing.T) *sql.DB {
	t.Helper()
	previous := *config.Config
	t.Cleanup(func() { *config.Config = previous })
	config.Config.BackendDB = "sqlite3"
	config.Config.SQLite3DataFile = filepath.Join(t.TempDir(), "forget.db")
	config.Config.SkipOrchestratorDatabaseUpdate = false
	backend, err := db.OpenOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	return backend
}

func TestForgetClusterWaitsForInFlightWrite(t *testing.T) {
	backend := setupForgetBackend(t)

	cluster := "forget-test:3306"
	insert := func(host string) {
		t.Helper()
		instance := &Instance{Key: InstanceKey{Hostname: host, Port: 3306}, ClusterName: cluster, Version: "5.7.0"}
		query, args, err := mkInsertOdkuForInstances([]*Instance{instance}, true, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecOrchestrator(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	insert("forget-test")

	// Model a discovery write which has passed its forgotten-instance check,
	// but has not yet committed a newly discovered member to the backend.
	releaseWrite := instanceWriteLocks.acquire([]string{clusterWriteLockKey(cluster)}, false)
	locked := true
	defer func() {
		if locked {
			releaseWrite()
		}
	}()
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		close(started)
		finished <- ForgetCluster(cluster)
	}()
	<-started
	select {
	case err := <-finished:
		t.Fatalf("forget completed before the in-flight write: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	insert("late-member")
	releaseWrite()
	locked = false
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forget did not complete after the write")
	}

	// Late members must be included in both the deletion and the forget cache.
	key := InstanceKey{Hostname: "late-member", Port: 3306}
	if !InstanceIsForgotten(&key) {
		t.Fatal("late member was not marked forgotten")
	}
	// Delayed discovery results must not restore the deleted cluster.
	for i := 0; i < 200; i++ {
		key := InstanceKey{Hostname: "forget-test", Port: 3306}
		if i%2 != 0 {
			key.Hostname = "late-member"
		}
		if err := WriteInstance(&Instance{Key: key, ClusterName: cluster, Version: "5.7.0"}, true, nil); err != nil {
			t.Fatalf("delayed write %d: %v", i, err)
		}
	}
	// A mixed buffered batch still persists the unrelated cluster while
	// dropping results for the forgotten cluster.
	otherCluster := "batch-unrelated:3306"
	batch := []*Instance{
		{Key: key, ClusterName: cluster, Version: "5.7.0"},
		{Key: InstanceKey{Hostname: "batch-unrelated", Port: 3306}, ClusterName: otherCluster, Version: "5.7.0"},
	}
	if err := writeManyInstances(batch, true, true); err != nil {
		t.Fatal(err)
	}
	var otherCount int
	if err := backend.QueryRow(`select count(*) from database_instance where cluster_name = ?`, otherCluster).Scan(&otherCount); err != nil || otherCount != 1 {
		t.Fatalf("mixed batch did not persist the unrelated instance: count=%d error=%v", otherCount, err)
	}
	var count int
	if err := backend.QueryRow(`select count(*) from database_instance where cluster_name = ?`, cluster).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("forgotten cluster has %d instances", count)
	}
}

func TestForgetDoesNotBlockUnrelatedWrites(t *testing.T) {
	for _, clusterForget := range []bool{true, false} {
		name := "instance"
		if clusterForget {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			setupForgetBackend(t)
			cluster := "isolated-" + name + ":3306"
			key := InstanceKey{Hostname: "isolated-" + name, Port: 3306}
			if err := WriteInstance(&Instance{Key: key, ClusterName: cluster, Version: "5.7.0"}, true, nil); err != nil {
				t.Fatal(err)
			}
			lockKey := instanceWriteLockKey(&key)
			if clusterForget {
				lockKey = clusterWriteLockKey(cluster)
			}
			release := instanceWriteLocks.acquire([]string{lockKey}, false)
			locked := true
			defer func() {
				if locked {
					release()
				}
			}()
			finished := make(chan error, 1)
			go func() {
				if clusterForget {
					finished <- ForgetCluster(cluster)
				} else {
					finished <- ForgetInstance(&key)
				}
			}()
			select {
			case err := <-finished:
				t.Fatalf("forget did not wait for the conflicting write: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			// For forget-instance even another member of the SAME cluster can write.
			otherCluster := cluster
			if clusterForget {
				otherCluster = "unrelated:3306"
			}
			written := make(chan error, 1)
			go func() {
				written <- WriteInstance(&Instance{Key: InstanceKey{Hostname: "unrelated-" + name, Port: 3306}, ClusterName: otherCluster, Version: "5.7.0"}, true, nil)
			}()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("forget blocked an unrelated write")
			}
			release()
			locked = false
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("forget did not complete")
			}
		})
	}
}

func TestInstanceWriteLocksOrderAndCleanup(t *testing.T) {
	set := instanceWriteLockSet{locks: make(map[string]*instanceWriteLock)}
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func(reverse bool) {
			defer workers.Done()
			keys := []string{"a", "b", "a"}
			if reverse {
				keys = []string{"b", "a", "b"}
			}
			for j := 0; j < 100; j++ {
				release := set.acquire(keys, true)
				release()
			}
		}(i%2 == 0)
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping lock acquisition deadlocked")
	}
	if len(set.locks) != 0 {
		t.Fatalf("unused locks retained: %d", len(set.locks))
	}
}
