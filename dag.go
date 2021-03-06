// Package dag is the base of a family of packages for working with directed acyclic graphs (DAGs)
// most (if not all) use cases assume the dag is a merkle-tree https://en.wikipedia.org/wiki/Merkle_tree
//
// dag package has two external dependencies that are worth developing an understanding of:
// * cid - github.com/ipfs/go-cid, github.com/ipld/specs
//   "Content IDentifiers" is a hashing technique that embeds additional info about the hash in question
// * ipld - github.com/ipfs/go-ipld-format, github.com/ipld/specs
//   ipld is a linked-data format for content-addressed data
//
// def check those out. dag package attempts to interoperate with these interfaces
// wherever & whenever possible, in the name of compatibility
package dag

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ugorji/go/codec"
)

// Node is a subset of the ipld ipld.Node interface, defining just the necessary
// bits the dag package works with
type Node interface {
	// pulled from blocks.Block format
	Cid() cid.Cid
	// Links is a helper function that returns all links within this object
	Links() []*ipld.Link
	// Size returns the size in bytes of the serialized object
	Size() (uint64, error)
}

var (
	// ErrIndexOutOfRange indicates the index given is out of range of the Manifest
	ErrIndexOutOfRange = fmt.Errorf("index out of range")

	// ErrIDNotFound indicates the id given is not found in the Manifest
	ErrIDNotFound = fmt.Errorf("id not found in Manifest")
)

// NewManifest generates a manifest from an ipld node
func NewManifest(ctx context.Context, ng ipld.NodeGetter, id cid.Cid) (*Manifest, error) {
	ms := &mstate{
		ctx:     ctx,
		ng:      ng,
		weights: map[string]int{},
		links:   [][2]string{},
		sizes:   map[string]uint64{},
		m:       &Manifest{},
	}

	err := ms.makeManifest(id)
	return ms.m, err
}

// Manifest is a determinsitc description of a complete directed acyclic graph.
// Analogous to bittorrent .magnet files, manifests contain no content, only a description of
// the structure of a graph (nodes and links)
//
// Manifests are built around a flat list of node identifiers (usually hashes) and a list of
// links. A link element is a tuple of [from,to] where from and to are indexes in the
// nodes list
//
// Manifests always describe the FULL graph, a root node and all it's descendants
//
// A valid manifest has the following properties:
// * supplying the same dag to the manifest function must be deterministic:
//   manifest_of_dag = manifest(dag)
//   hash(manifest_of_dag) == hash(manifest(dag))
// * In order to generate a manifest, you need the full DAG
// * The list of nodes MUST be sorted by number of descendants. When two nodes
//   have the same number of descenants, they MUST be sorted lexographically by node ID.
//   The means the root of the DAG will always be the first index
//
// Manifests are intentionally limited in scope to make them easier to prove, faster to calculate, hard requirement the list of nodes can be
// used as a base other structures can be built upon.
// by keeping manifests at a minimum they are easier to verify, forming a
// foundation for
type Manifest struct {
	Links [][2]int `json:"links"` // links between nodes
	Nodes []string `json:"nodes"` // list if CIDS contained in the DAG
}

// RootCID returns the root node as a CID. If for some reason the manifest is empty
// or the root hash isn't a valid CID, RootCID returns cid.Undef
func (m *Manifest) RootCID() cid.Cid {
	if len(m.Nodes) == 0 {
		return cid.Undef
	}
	id, err := cid.Parse(m.Nodes[0])
	if err != nil {
		return cid.Undef
	}
	return id
}

// IDIndex returns the node index of the id
func (m *Manifest) IDIndex(id string) int {
	for i, node := range m.Nodes {
		if node == id {
			return i
		}
	}
	return -1
}

// // SubDAGIndex lists all hashes that are a descendant of manifest node index
// func (m *Manifest) SubDAGIndex(idx int, nodes *[]string) {
// 	// for i, l := range m.Links {
// 	// 	if l[0] == idx {

// 	// 	}
// 	// }
// }

// MarshalCBOR encodes this manifest as CBOR data
func (m *Manifest) MarshalCBOR() (data []byte, err error) {
	buf := &bytes.Buffer{}
	err = codec.NewEncoder(buf, &codec.CborHandle{}).Encode(m)
	data = buf.Bytes()
	return
}

// UnmarshalCBORManifest decodes a manifest from a byte slice
func UnmarshalCBORManifest(data []byte) (m *Manifest, err error) {
	m = &Manifest{}
	err = codec.NewDecoder(bytes.NewReader(data), &codec.CborHandle{}).Decode(m)
	return
}

type sortableLinks [][2]int

func (sl sortableLinks) Len() int { return len(sl) }
func (sl sortableLinks) Less(i, j int) bool {
	return (1000*(sl[i][0]+1) + (sl[i][1])) < (1000*(sl[j][0]+1) + (sl[j][1]))
}
func (sl sortableLinks) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }

// mstate is a state machine for generating a manifest
type mstate struct {
	ctx     context.Context
	ng      ipld.NodeGetter
	weights map[string]int // map of already-added cids to weight (descendant count)
	links   [][2]string
	sizes   map[string]uint64
	m       *Manifest
}

func (ms *mstate) makeManifest(id cid.Cid) error {
	node, err := ms.ng.Get(ms.ctx, id)
	if err != nil {
		return err
	}

	weight := 0
	if err := ms.addNode(node, &weight); err != nil {
		return err
	}

	// alpha sort keys
	sort.StringSlice(ms.m.Nodes).Sort()
	// then sort by weight
	sort.Sort(ms)

	// at this point indexes are set, re-use weights map to hold indicies
	for i, id := range ms.m.Nodes {
		ms.weights[id] = i
	}

	var sl sortableLinks
	for _, link := range ms.links {
		from, to := link[0], link[1]
		sl = append(sl, [2]int{ms.weights[from], ms.weights[to]})
	}
	sort.Sort(sl)
	ms.m.Links = ([][2]int)(sl)

	return nil
}

// mstate implements the sort interface to sort Manifest nodes by weights
func (ms *mstate) Len() int           { return len(ms.sizes) }
func (ms *mstate) Less(a, b int) bool { return ms.weights[ms.m.Nodes[a]] > ms.weights[ms.m.Nodes[b]] }
func (ms *mstate) Swap(i, j int)      { ms.m.Nodes[j], ms.m.Nodes[i] = ms.m.Nodes[i], ms.m.Nodes[j] }

// addNode places a node in the manifest & state machine, recursively adding linked nodes
// addNode returns early if this node is already added to the manifest
// note (b5): this is one of my fav techniques. I ship hard for pointer outparams + recursion
func (ms *mstate) addNode(node Node, weight *int) (err error) {
	id := node.Cid().String()
	if _, ok := ms.sizes[id]; ok {
		return nil
	}

	ms.m.Nodes = append(ms.m.Nodes, id)
	lWeight := 0

	ms.sizes[id], err = node.Size()
	if err != nil {
		return
	}

	for _, link := range node.Links() {
		*weight++

		linkNode, err := link.GetNode(ms.ctx, ms.ng)
		if err != nil {
			return err
		}
		ms.links = append(ms.links, [2]string{id, linkNode.Cid().String()})

		lWeight = 0
		if err = ms.addNode(linkNode, &lWeight); err != nil {
			return err
		}

		*weight += lWeight
	}

	ms.weights[id] = *weight
	return nil
}

// NewInfo creates an info with an underlying manifest
func NewInfo(ctx context.Context, ng ipld.NodeGetter, id cid.Cid) (*Info, error) {
	ms := &mstate{
		ctx:     ctx,
		ng:      ng,
		weights: map[string]int{},
		links:   [][2]string{},
		sizes:   map[string]uint64{},
		m:       &Manifest{},
	}

	err := ms.makeManifest(id)
	if err != nil {
		return nil, err
	}

	var sizes []uint64
	for _, id := range ms.m.Nodes {
		sizes = append(sizes, ms.sizes[id])
	}

	di := &Info{
		Manifest: ms.m,
		Sizes:    sizes,
	}

	return di, nil
}

// Info is os.FileInfo for dags: a struct that describes important
// details about a graph. Info builds on a manifest.
//
// when being sent over the network, the contents of Info should be considered gossip,
// as Info's are *not* deterministic. This has important implications
// Info should contain application-specific info about a datset
type Info struct {
	// Info is built upon a manifest
	Manifest *Manifest      `json:"manifest"`
	Labels   map[string]int `json:"labels,omitempty"` // sections are lists of logical sub-DAGs by positions in the nodes list
	Sizes    []uint64       `json:"sizes,omitempty"`  // sizes of nodes in bytes
}

// AddLabel adds a label to the list of Info.Labels
// it returns an error if the index is out of bounds
func (i *Info) AddLabel(label string, index int) error {
	if index < 0 || index >= len(i.Manifest.Nodes) {
		return ErrIndexOutOfRange
	}
	if i.Labels == nil {
		i.Labels = map[string]int{}
	}
	i.Labels[label] = index
	return nil
}

// AddLabelByID adds a label to the list of Info.Labels
// it returns an error if the id is not part of the DAG
func (i *Info) AddLabelByID(label, id string) error {
	index := i.Manifest.IDIndex(id)
	if index == -1 {
		return ErrIDNotFound
	}
	return i.AddLabel(label, index)
}

// RootCID proxies the manifest RootCID method, protecting against situations where
// the underlying manifest doesn't exist
func (i *Info) RootCID() cid.Cid {
	if i.Manifest == nil {
		return cid.Undef
	}
	return i.Manifest.RootCID()
}

// MarshalCBOR encodes a dag.Info as CBOR data
func (i *Info) MarshalCBOR() (data []byte, err error) {
	buf := &bytes.Buffer{}
	err = codec.NewEncoder(buf, &codec.CborHandle{}).Encode(i)
	data = buf.Bytes()
	return
}

// UnmarshalCBORDagInfo decodes an Info from a byte slice
func UnmarshalCBORDagInfo(data []byte) (i *Info, err error) {
	i = &Info{}
	err = codec.NewDecoder(bytes.NewReader(data), &codec.CborHandle{}).Decode(i)
	return
}

// Completion tracks the presence of blocks described in a manifest
// Completion can be used to store transfer progress, or be stored as a record
// of which blocks in a DAG are missing
// each element in the slice represents the index a block in a manifest.Nodes field,
// which contains the hash of a block needed to complete a manifest
// the element in the progress slice represents the transmission completion of that block
// locally. It must be a number from 0-100, 0 = nothing locally, 100 = block is local.
// note that progress is not necessarily linear. for example the following is 50% complete progress:
//
// manifest.Nodes: ["QmA", "QmB", "QmC", "QmD"]
// progress:       [0, 100, 0, 100]
//
type Completion []uint16

// NewCompletion constructs a progress from
func NewCompletion(mfst, missing *Manifest) Completion {
	// fill in progress
	prog := make(Completion, len(mfst.Nodes))
	for i := range prog {
		prog[i] = 100
	}

	// then set missing blocks to 0
	for _, miss := range missing.Nodes {
		for i, hash := range mfst.Nodes {
			if hash == miss {
				prog[i] = 0
			}
		}
	}

	return prog
}

// Percentage expressess the completion as a floating point number betwen 0.0 and 1.0
func (p Completion) Percentage() (pct float32) {
	for _, bl := range p {
		pct += float32(bl) / float32(100)
	}
	return (pct / float32(len(p)))
}

// CompletedBlocks returns the number of blocks that are completed
func (p Completion) CompletedBlocks() (count int) {
	for _, bl := range p {
		if bl == 100 {
			count++
		}
	}
	return count
}

// Complete returns weather progress is finished
func (p Completion) Complete() bool {
	for _, bl := range p {
		if bl != 100 {
			return false
		}
	}
	return true
}
