package dsync

import (
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	options "github.com/ipfs/interface-go-ipfs-core/options"
)

// NewLocalNodeGetter creates a local NodeGetter from a ipfs CoreAPI instance
// "local" NodeGetters don't fetch over the dweb.
//
// it's important to pass Dsync a NodeGetter instance that doesn't perform any
// network operations when trying to resolve blocks. If we don't do this dsync
// will ask ipfs for blocks it doesn't have, and ipfs will try to *fetch*
// this blocks, which kinda defeats the point of syncing blocks by other means
func NewLocalNodeGetter(api coreiface.CoreAPI) (ipld.NodeGetter, error) {
	// return merkledag.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore))), nil
	noFetchBlocks, err := api.WithOptions(options.Api.FetchBlocks(false))
	if err != nil {
		return nil, err
	}
	return noFetchBlocks.Dag(), nil
}
