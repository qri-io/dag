// Package dsync implements point-to-point block-syncing between a local and remote source.
// It's like rsync, but specific to merkle-dags
//
// How Dsync Works On The Local Side
//
// Fetch
//
// The local creates a fetch using `NewFetch` (or `NewFetchWithManifest`, if the local already has the Manifest of the DAG at `id`).
//
//  The local starts the fetch process by using `fetch.Do()`. It:
//
//  1) requests the Manifest of the DAG at `id`
//  2) determines which blocks it needs to ask the remote for, and creates a diff Manifest (note: right now, because of
//     the way we are able to access the block level storage, we can't actually produce a meaningful diff, so the diff is
//     actually just the full Manifest)
//  3) creates a number of fetchers to fetch the blocks in parallel.
//  4) sets up processes to coordinate the fetch responses
//
//  Fetch.Do() ends when:
//  - the process has timed out
//  - some response has returned an error
//  - the local has successfully fetched all the blocks needed to complete the DAG
//
// Send
//
// The local has a DAG it wants to send to a remote. The local creates a `send` using `NewSend` and a Manifest.
//
//  The local starts the send process by using `send.Do()`. It:
//
//  1) requests a send using `remote.ReqSend`. It gets back a send id (`sid`) and the `diff` the Manifest. The `diff`
//     Manifest lists all the blocks ids remote needs in order to have the full DAG (note: right now, the diff is always
//     the full Manifest)
//  2) creates a number of senders to send or re-send the blocks in parallel
//  3) sets up processes to coordiate the send responses
//
//  Send.Do() ends when:
//  - the process has timed out
//  - the local has have attempted to many re-tries
//  - some response from the remote returns an error
//  - the local successfully sent the full DAG to the remote
//
// How Dsync Works On The Remote Side
//
// The remote does not keep track of (or coordinate) fetch and it keeps only cursory track of send.
//
// Instead, the local uses the `Remote` interface (`ReqSend`, `PutBlocks`, `ReqManifest`, `GetBlock` methods) to communicate with the remote. Currently, we have an implimentation of the `Remote` interface that communicates over HTTP. We use `HTTPRemote` on the local to send requests and `Receivers` on the remote to handle and respond to them.
//
// `HTTPRemote` structures requests correctly to work with the remote's Receiver http api. And `HTTPHandler` exposes that http api, so it can handle requests from the local.
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
