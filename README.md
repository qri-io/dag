# dag
--
    import "github.com/qri-io/dag"

Package dag is the base of a family of packages for working with directed
acyclic graphs (DAGs) most (if not all) use cases assume the dag is a
merkle-tree https://en.wikipedia.org/wiki/Merkle_tree

## Usage

#### type Completion

```go
type Completion []uint16
```

Completion tracks the presence of blocks described in a manifest Completion can
be used to store transfer progress, or be stored as a record of which blocks in
a DAG are missing each element in the slice represents the index a block in a
manifest.Nodes field, which contains the hash of a block needed to complete a
manifest the element in the progress slice represents the transmission
completion of that block locally. It must be a number from 0-100, 0 = nothing
locally, 100 = block is local. note that progress is not necessarily linear. for
example the following is 50% complete progress:

manifest.Nodes: ["QmA", "QmB", "QmC", "QmD"] progress: [0, 100, 0, 100]

#### func  NewCompletion

```go
func NewCompletion(mfst, missing *Manifest) Completion
```
NewCompletion constructs a progress from

#### func (Completion) Complete

```go
func (p Completion) Complete() bool
```
Complete returns weather progress is finished

#### func (Completion) CompletedBlocks

```go
func (p Completion) CompletedBlocks() (count int)
```
CompletedBlocks returns the number of blocks that are completed

#### func (Completion) Percentage

```go
func (p Completion) Percentage() (pct float32)
```
Percentage expressess the completion as a floating point number betwen 0.0 and
1.0

#### type Info

```go
type Info struct {
	// Info is built upon a manifest
	Manifest *Manifest      `json:"manifest"`
	Paths    map[string]int `json:"paths,omitempty"` // sections are lists of logical sub-DAGs by positions in the nodes list
	Sizes    []uint64       `json:"sizes,omitempty"` // sizes of nodes in bytes
}
```

Info is os.FileInfo for dags: a struct that describes important details about a
graph. Info builds on a manifest

when being sent over the network, the contents of Info should be considered
gossip, as Info's are *not* deterministic. This has important implications Info
should contain application-specific info about a datset

#### func  NewInfo

```go
func NewInfo(ctx context.Context, ng ipld.NodeGetter, id cid.Cid) (*Info, error)
```
NewInfo creates a

#### type Manifest

```go
type Manifest struct {
	Links [][2]int `json:"links"` // links between nodes
	Nodes []string `json:"nodes"` // list if CIDS contained in the DAG
}
```

Manifest is a determinsitc description of a complete directed acyclic graph.
Analogous to bittorrent .magnet files, manifests contain no content, only a
description of the structure of a graph (nodes and links)

Manifests are built around a flat list of node identifiers (usually hashes) and
a list of links. A link element is a tuple of [from,to] where from and to are
indexes in the nodes list

Manifests always describe the FULL graph, a root node and all it's descendants

A valid manifest has the following properties: * supplying the same dag to the
manifest function must be deterministic:

    manifest_of_dag = manifest(dag)
    hash(manifest_of_dag) == hash(manifest(dag))

* In order to generate a manifest, you need the full DAG * The list of nodes
MUST be sorted by number of descendants. When two nodes

    have the same number of descenants, they MUST be sorted lexographically by node ID.
    The means the root of the DAG will always be the first index

Manifests are intentionally limited in scope to make them easier to prove,
faster to calculate, hard requirement the list of nodes can be used as a base
other structures can be built upon. by keeping manifests at a minimum they are
easier to verify, forming a foundation for

#### func  Missing

```go
func Missing(ctx context.Context, ng ipld.NodeGetter, m *Manifest) (missing *Manifest, err error)
```
Missing returns a manifest describing blocks that are not in this node for a
given manifest

#### func  NewManifest

```go
func NewManifest(ctx context.Context, ng ipld.NodeGetter, id cid.Cid) (*Manifest, error)
```
NewManifest generates a manifest from an ipld node

#### func  UnmarshalCBORManifest

```go
func UnmarshalCBORManifest(data []byte) (m *Manifest, err error)
```
UnmarshalCBORManifest decodes a manifest from a byte slice

#### func (*Manifest) MarshalCBOR

```go
func (m *Manifest) MarshalCBOR() (data []byte, err error)
```
MarshalCBOR encodes this manifest as CBOR data

#### type Node

```go
type Node interface {
	// pulled from blocks.Block format
	Cid() cid.Cid
	// Links is a helper function that returns all links within this object
	Links() []*ipld.Link
	// Size returns the size in bytes of the serialized object
	Size() (uint64, error)
}
```

Node is a subset of the ipld ipld.Node interface, defining just the necessary
bits the dag package works with

#### type NodeGetter

```go
type NodeGetter struct {
	Dag coreiface.DagAPI
}
```

NodeGetter wraps the go-ipfs DagAPI to satistfy the IPLD NodeGetter interface

#### func (*NodeGetter) Get

```go
func (ng *NodeGetter) Get(ctx context.Context, id cid.Cid) (ipld.Node, error)
```
Get retrieves nodes by CID. Depending on the NodeGetter implementation, this may
involve fetching the Node from a remote machine; consider setting a deadline in
the context.

#### func (*NodeGetter) GetMany

```go
func (ng *NodeGetter) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption
```
GetMany returns a channel of NodeOptions given a set of CIDs.
