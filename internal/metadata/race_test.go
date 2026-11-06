package metadata

import (
	"context"
	"sync"
	"testing"
)

func TestConcurrentUpsertsAndDeletion(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	ctx := context.Background()
	store := NewStore(pool)
	migID := mustCreateMigration(ctx, t, pool)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = store.UpsertObjectState(ctx, migID, "race-key", 1, func(o *ObjectState) error {
				o.DestVersionID = "v-dest"
				o.MigrationState = "migrated"
				return nil
			})
		}(i)
	}
	wg.Wait()

	state, err := store.GetObjectState(ctx, migID, "race-key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if state.Generation < 1 || state.Generation > 11 {
		t.Fatalf("unexpected generation %d", state.Generation)
	}

	if err := store.MarkDeleted(ctx, migID, "race-key"); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}
	state, err = store.GetObjectState(ctx, migID, "race-key")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if !state.Deleted {
		t.Fatal("expected tombstone after deletion")
	}
}
