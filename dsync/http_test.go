package dsync

import (
	"context"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"

	files "github.com/ipfs/go-ipfs-files"
	"github.com/qri-io/dag"
)

func TestSyncHTTP(t *testing.T) {

	ctx := context.Background()
	_, a, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, b, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// yooooooooooooooooooooo
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y" + strings.Repeat("o", 350))))
	path, err := a.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}

	aGetter := &dag.NodeGetter{Dag: a.Dag()}
	info, err := dag.NewInfo(ctx, aGetter, path.Cid())
	if err != nil {
		t.Fatal(err)
	}

	onCompleteCalled := make(chan struct{}, 1)
	onCompleteHook := func(_ context.Context, _ dag.Info, _ map[string]string) error {
		onCompleteCalled <- struct{}{}
		return nil
	}

	removeCheckCalled := make(chan struct{}, 1)
	removeCheckHook := func(_ context.Context, _ dag.Info, _ map[string]string) error {
		removeCheckCalled <- struct{}{}
		return nil
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	bdsync, err := New(bGetter, b.Block(), func(cfg *Config) {
		cfg.AllowRemoves = true
		cfg.PushPreCheck = func(context.Context, dag.Info, map[string]string) error { return nil }
		cfg.PushComplete = onCompleteHook
		cfg.RemoveCheck = removeCheckHook
	})
	if err != nil {
		t.Fatal(err)
	}

	s := httptest.NewServer(HTTPRemoteHandler(bdsync))
	defer s.Close()

	cli := &HTTPClient{URL: s.URL + "/dsync"}

	push, err := NewPush(aGetter, info, cli, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := push.Do(ctx); err != nil {
		t.Error(err)
	}

	// b should now be able to generate a manifest
	_, err = dag.NewManifest(ctx, bGetter, path.Cid())
	if err != nil {
		t.Error(err)
	}

	<-onCompleteCalled

	if err := cli.RemoveCID(ctx, info.RootCID().String(), nil); err != nil {
		t.Error(err)
	}

	<-removeCheckCalled
}

func TestRemoveNotSupported(t *testing.T) {
	ctx := context.Background()

	_, b, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	bdsync, err := New(bGetter, b.Block())
	if err != nil {
		t.Fatal(err)
	}

	s := httptest.NewServer(HTTPRemoteHandler(bdsync))
	defer s.Close()

	cli := &HTTPClient{URL: s.URL + "/dsync"}
	if err := cli.RemoveCID(ctx, "foo", nil); err != ErrRemoveNotSupported {
		t.Errorf("expected error remoce not supported, got: %s", err.Error())
	}
}
