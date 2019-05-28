package dsync

import (
	"context"
	"testing"

	"github.com/qri-io/dag"
)

func TestSend(t *testing.T) {
	ctx := context.Background()
	a, b := newLocalRemoteIPFSAPI(ctx, t)
	id := addOneBlockDAG(a, t)

	aGetter := &dag.NodeGetter{Dag: a.Dag()}
	mfst, err := dag.NewManifest(ctx, aGetter, id)
	if err != nil {
		t.Fatal(err)
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	receive, err := NewTransfer(ctx, bGetter, b.Block(), mfst)
	if err != nil {
		t.Fatal(err)
	}

	rem := &remote{
		receive: receive,
		lng:     bGetter,
		bapi:    b.Block(),
	}

	send, err := NewPush(ctx, aGetter, mfst, rem)
	if err != nil {
		t.Fatal(err)
	}

	if err := send.Do(); err != nil {
		t.Error(err)
	}

	// b should now be able to generate a manifest
	_, err = dag.NewManifest(ctx, bGetter, id)
	if err != nil {
		t.Error(err)
	}
}
