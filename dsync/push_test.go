package dsync

import (
	"context"
	"testing"

	"github.com/qri-io/dag"
)

func TestPush(t *testing.T) {
	ctx := context.Background()
	a, b := newLocalRemoteIPFSAPI(ctx, t)
	id := addOneBlockDAG(a, t)

	aGetter := &dag.NodeGetter{Dag: a.Dag()}
	mfst, err := dag.NewManifest(ctx, aGetter, id)
	if err != nil {
		t.Fatal(err)
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	rem := New(bGetter, b.Block())

	send, err := NewPush(aGetter, mfst, rem)
	if err != nil {
		t.Fatal(err)
	}

	if err := send.Do(ctx); err != nil {
		t.Error(err)
	}

	// b should now be able to generate a manifest
	_, err = dag.NewManifest(ctx, bGetter, id)
	if err != nil {
		t.Error(err)
	}
}
