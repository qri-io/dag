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
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
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

	f := files.NewReaderFile("oh_hey", "oh_hey", ioutil.NopCloser(strings.NewReader("y"+strings.Repeat("o", 35000))), nil)
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
	rs := NewReceivers(ctx, bGetter, b.Block())
	s := httptest.NewServer(rs.HTTPHandler())
	defer s.Close()

	rem := &HTTPRemote{URL: s.URL}

	send, err := NewSend(ctx, aGetter, mfst, rem)
	if err != nil {
		t.Fatal(err)
	}

	if err := send.Do(); err != nil {
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
	receive *Receive
	lng     ipld.NodeGetter
	bapi    coreiface.BlockAPI
}

func (r *remote) ReqSend(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
	ctx := context.Background()
	r.receive, err = NewReceive(ctx, r.lng, r.bapi, mfst)
	if err != nil {
		return
	}
	sid = r.receive.sid
	diff = r.receive.diff
	return
}

func (r *remote) PutBlock(sid, hash string, data []byte) Response {
	return r.receive.ReceiveBlock(hash, bytes.NewReader(data))
}

func (r *remote) ReqManifest(ctx context.Context, hash string) (mfst *dag.Manifest, err error) {
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
