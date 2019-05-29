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
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	ipld "github.com/ipfs/go-ipld-format"
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

	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y"+strings.Repeat("o", 350))))
	path, err := a.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}

	aGetter := &dag.NodeGetter{Dag: a.Dag()}
	mfst, err := dag.NewManifest(ctx, aGetter, path.Cid())
	if err != nil {
		t.Fatal(err)
	}

	bGetter := &dag.NodeGetter{Dag: b.Dag()}
	ts := New(bGetter, b.Block())
	s := httptest.NewServer(HTTPRemoteHandler(ts))
	defer s.Close()

	cli := &HTTPClient{URL: s.URL}

	push, err := NewPush(aGetter, mfst, cli)
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
	receive *Session
	lng     ipld.NodeGetter
	bapi    coreiface.BlockAPI
}

func (r *remote) PushStart(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
	ctx := context.Background()
	r.receive, err = NewSession(ctx, r.lng, r.bapi, mfst)
	if err != nil {
		return
	}
	sid = r.receive.sid
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
