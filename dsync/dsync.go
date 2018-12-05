// Package dsync implements point-to-point block-syncing between a local and remote source
// it's like rsync, but specific to merkle-dags
package dsync

const (
	// default to parallelism of 3. So far 4 was enough to blow up a std k8s pod running IPFS :(
	defaultSendParallelism = 3
	// default to parallelism of 3
	// TODO (b5): tune this figure
	defaultFetchParallelism = 3
	// total number of retries to attempt before send is considered faulty
	// TODO (b5): this number should be retries *per object*, and a much lower
	// number, like 5.
	maxRetries = 25
)

// TODO (b5): WIP. lots of things could just become methods on a "local":
// local.NewSendReq(remote).Do()
// local.NewFetchReq(remote).Do()
// local could optionally add a receiver to act as a remote for inboud sync requests
//
// Local encapsulates sync operations from the end of the wire a process owns
// type Local struct {
// 	ctx context.Context
// 	lng ipld.NodeGetter
// 	bapi coreiface.BlockAPI
// }
