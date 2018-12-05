package dag

import (
	"context"
	"testing"
)

func TestMemInfoStore(t *testing.T) {
	is := NewMemInfoStore()
	t.Run("Interface", testInfoStoreInterface(is))
}

// TODO (b5): I'm working on getting this into something either copy-pastable or importable
func testInfoStoreInterface(is InfoStore) func(t *testing.T) {
	ctx := context.Background()
	g := newGraph([]layer{{8, 2 * kb}})
	ng := TestingNodeGetter{g}

	return func(t *testing.T) {
		info, err := NewInfo(ctx, ng, g[0].Cid())
		if err != nil {
			t.Fatal(err)
		}

		if _, err := is.DAGInfo(ctx, ""); err != ErrInfoNotFound {
			t.Errorf("expected ErrInfoNotFound for a get to a non-existent ID. got: %s", err)
		}
		if deleted, err := is.DeleteDAGInfo(ctx, ""); err != nil {
			t.Errorf("delete to non-existent key shouldn't error")
		} else if deleted {
			t.Errorf("expected empty delete to return false deleted value")
		}

		if err := is.PutDAGInfo(ctx, info.RootCID().String(), info); err != nil {
			t.Errorf("error putting info: %s", err.Error())
		}

		if err := is.PutDAGInfo(ctx, "foo", info); err != nil {
			t.Errorf("error putting info with arbitrary id key: %s", err.Error())
		}

		info, err = is.DAGInfo(ctx, info.RootCID().String())
		if err != nil {
			t.Errorf("error getting info: %s", err.Error())
		}

		if deleted, err := is.DeleteDAGInfo(ctx, info.RootCID().String()); err != nil {
			t.Errorf("error deleting info: %s", err.Error())
		} else if !deleted {
			t.Errorf("expected deleted to return true")
		}

		if deleted, err := is.DeleteDAGInfo(ctx, "foo"); err != nil {
			t.Errorf("error deleting arbitrary key: %s", err.Error())
		} else if !deleted {
			t.Errorf("expected dleted to equal true")
		}
	}
}
