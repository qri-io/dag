package ipfs_core_http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
	caopts "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface/options"
)

// BlockAPI is the core interface for working with raw IPFS blocks,
// the `ipfs block` section of the go-ipfs CLI
type BlockAPI CoreHTTP

// BlockStat groups details about a block
type BlockStat struct {
	path coreiface.ResolvedPath
	size int
}

// BlockStat must satistfy coreiface.BlockStat
var _ coreiface.BlockStat = (*BlockStat)(nil)

// Size returns the size of the block
func (b BlockStat) Size() int { return b.size }

// Path gives the resolved path of the block
func (b BlockStat) Path() coreiface.ResolvedPath { return b.path }

// assert Core interface is satisfied
var _ coreiface.BlockAPI = (*BlockAPI)(nil)

// Put imports raw block data, hashing it using specified settings.
func (c BlockAPI) Put(ctx context.Context, src io.Reader, opts ...caopts.BlockPutOption) (coreiface.BlockStat, error) {
	// TODO (b5): implement options
	// pref, err := caopts.BlockPutOptions(opts...)
	// if err != nil {
	// 	return nil, err
	// }

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	w, err := writer.CreateFormFile("file", "")
	if err != nil {
		return nil, err
	}

	dataLen, err := io.Copy(w, src)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/block/put", c.url), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())

	res, err := c.cli.Do(req)
	if err != nil {
		return nil, err
	}

	env := struct {
		Key  string
		Size int
	}{}

	if err := json.NewDecoder(res.Body).Decode(&env); err != nil {
		return nil, err
	}
	res.Body.Close()

	id, err := cid.Parse(env.Key)
	if err != nil {
		return nil, err
	}

	return &BlockStat{path: coreiface.IpldPath(id), size: int(dataLen)}, nil
}

// Get attempts to resolve the path and return a reader for data in the block
func (c BlockAPI) Get(context.Context, coreiface.Path) (io.Reader, error) {
	return nil, fmt.Errorf("not finished")
}

// Rm removes the block specified by the path from local blockstore.
// By default an error will be returned if the block can't be found locally.
//
// NOTE: If the specified block is pinned it won't be removed and no error
// will be returned
func (c BlockAPI) Rm(context.Context, coreiface.Path, ...caopts.BlockRmOption) error {
	return fmt.Errorf("not finished")
}

// Stat returns information on
func (c BlockAPI) Stat(context.Context, coreiface.Path) (coreiface.BlockStat, error) {
	return nil, fmt.Errorf("not finished")
}
