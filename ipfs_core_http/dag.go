package ipfs_core_http

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	blocks "gx/ipfs/QmRcHuYzAyswytBuMF78rj3LTChYszomRFXNg4685ZN1WM/go-block-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
	caopts "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface/options"
)

// DagAPI is the core interface for working with ipld DAG objects,
// the `ipfs dag` section of the go-ipfs CLI
type DagAPI CoreHTTP

// DagAPI must implement coreiapi.DagAPI
var _ coreiface.DagAPI = (*DagAPI)(nil)

// Put inserts data using specified format and input encoding.
// Unless used with WithCodec or WithHash, the defaults "dag-cbor" and
// "sha256" are used.
func (d DagAPI) Put(ctx context.Context, src io.Reader, opts ...caopts.DagPutOption) (coreiface.ResolvedPath, error) {
	return nil, fmt.Errorf("not finished")
}

// Get attempts to resolve and get the node specified by the path
func (d DagAPI) Get(ctx context.Context, path coreiface.Path) (ipld.Node, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/dag/get?arg=%s", d.url, path.String()), nil)
	if err != nil {
		return nil, err
	}

	res, err := d.cli.Do(req)
	if err != nil {
		return nil, err
	}

	rawdata, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	res.Body.Close()

	node, err := ipld.Decode(blocks.NewBlock(rawdata))
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Tree returns list of paths within a node specified by the path.
func (d DagAPI) Tree(ctx context.Context, path coreiface.Path, opts ...caopts.DagTreeOption) ([]coreiface.Path, error) {
	return nil, fmt.Errorf("not finished")
}

// Batch creates new DagBatch
func (d DagAPI) Batch(ctx context.Context) coreiface.DagBatch {
	return nil
}
