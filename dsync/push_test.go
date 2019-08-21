package dsync

import (
	"context"
	"testing"

	"github.com/qri-io/dag"
)

func TestPush(t *testing.T) {
	dpc := DefaultDagPrecheck
	defer func() { DefaultDagPrecheck = dpc }()
	DefaultDagPrecheck = func(context.Context, dag.Info, map[string]string) error { return nil }

	ctx := context.Background()
	a, b := newLocalRemoteIPFSAPI(ctx, t)
	id := addOneBlockDAG(a, t)

	aGetter := &dag.NodeGetter{Dag: a.Dag()}
	info, err := dag.NewInfo(ctx, aGetter, id)
	if err != nil {
		t.Fatal(err)
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	rem, err := New(bGetter, b.Block())
	if err != nil {
		t.Fatal(err)
	}

	send, err := NewPush(aGetter, info, rem, false)
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
