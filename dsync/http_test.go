package dsync

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/qri-io/dag"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
)

func TestSyncHTTP(t *testing.T) {
	dpc := DefaultDagPrecheck
	defer func() { DefaultDagPrecheck = dpc }()
	DefaultDagPrecheck = func(context.Context, dag.Info) error { return nil }

	ctx := context.Background()
	_, a, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, b, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

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

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	ts, err := New(bGetter, b.Block())
	if err != nil {
		t.Fatal(err)
	}

	s := httptest.NewServer(HTTPRemoteHandler(ts))
	defer s.Close()

	cli := &HTTPClient{URL: s.URL}

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
}

// remote implements the Remote interface on a single receive session at a time
type remote struct {
	receive *session
	lng     ipld.NodeGetter
	bapi    coreiface.BlockAPI
}

func (r *remote) PushStart(info *dag.Info) (sid string, diff *dag.Manifest, err error) {
	ctx := context.Background()
	r.receive, err = newSession(ctx, r.lng, r.bapi, info, false, false)
	if err != nil {
		return
	}
	sid = r.receive.id
	diff = r.receive.diff
	return
}

func (r *remote) PushBlock(sid, hash string, data []byte) ReceiveResponse {
	return r.receive.ReceiveBlock(hash, bytes.NewReader(data))
}

func (r *remote) PullManifest(ctx context.Context, hash string) (mfst *dag.Manifest, err error) {
	id, err := cid.Parse(hash)
	if err != nil {
		return nil, err
	}

	return dag.NewManifest(ctx, r.lng, id)
}

func (r *remote) GetBlock(ctx context.Context, hash string) ([]byte, error) {
	id, err := cid.Parse(hash)
	if err != nil {
		return nil, err
	}

	node, err := r.lng.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	return node.RawData(), nil
}
