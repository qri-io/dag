// Package ipfs_core_http implements the core_api interface
// currently we're implementing on an as-needed basis
package ipfs_core_http

import (
	"net/http"

	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
)

// CoreHTTP implements github.com/ipfs/go-ipfs/core/coreapi/interface.CoreAPI
// by talking to an IPFS daemon via the HTTP API
type CoreHTTP struct {
	url string
	cli *http.Client
}

// NewCoreHTTP creates a core HTTP module
func NewCoreHTTP(urlstr string) *CoreHTTP {
	return &CoreHTTP{
		url: urlstr,
		cli: http.DefaultClient,
	}
}

// Block returns the BlockAPI interface implementation backed by the go-ipfs node
func (api CoreHTTP) Block() coreiface.BlockAPI {
	return (BlockAPI)(api)
}
