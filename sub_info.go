package dag

import (
	"fmt"
)

// subDAGGenerator is the state machine used to get a subDAG from a DAG
type subDAGGenerator struct {
	// RootIndex is the index of a node the Manifest that is to be the root the new dag
	RootIndex int
	// Conversion is a map keeping track of a previous index and its new counterpart
	Conversion map[int]int
	// PrevManifest is the previous Manifest, the larger dag out of which a sub dag gets created
	PrevManifest *Manifest
	// Manifest is the sub dag being generated
	Manifest *Manifest
	// CurrentParentIndex the index of the node whose links are currently being added to the manifest
	CurrentParentIndex int
	// Parents is the list of parents nodes whose links and children need to be added to the dag
	Parents []int
	// LastBranchIndex is the index of the last node that has children. It is used to determine if a node should be added to the Parentslist
	LastBranchIndex int
	// PrevSizes is the list of sizes from the original dag Info
	PrevSizes []uint64
	// Sizes is the list of sizes of the new sub dag Info
	Sizes []uint64
	// InverseLabels is the map of labels from the original dag Info, inversed - with the original node index as the key and the hash as the value
	InverseLabels map[int]string
	// Labels is the map of labels for the new dag Info
	Labels map[string]int
}

func newSubDAGGenerator(prevInfo *Info, root int) *subDAGGenerator {
	m := prevInfo.Manifest
	InverseLabels := map[int]string{}
	for path, index := range prevInfo.Labels {
		InverseLabels[index] = path
	}
	lastBranchIndex := 0
	if m != nil && len(m.Links) != 0 {
		lastBranchIndex = m.Links[len(m.Links)-1][0]
	}

	return &subDAGGenerator{
		Conversion:         map[int]int{},
		PrevManifest:       m,
		Manifest:           &Manifest{},
		CurrentParentIndex: 0,
		Parents:            []int{root},
		RootIndex:          root,
		LastBranchIndex:    lastBranchIndex,
		PrevSizes:          prevInfo.Sizes,
		Sizes:              []uint64{},
		InverseLabels:      InverseLabels,
		Labels:             map[string]int{},
	}
}

func (s *subDAGGenerator) addIndexToSubDAG(index int) {
	convertIndex := len(s.Manifest.Nodes)
	id := s.PrevManifest.Nodes[index]
	s.Manifest.Nodes = append(s.Manifest.Nodes, id)
	if s.PrevSizes != nil {
		s.Sizes = append(s.Sizes, s.PrevSizes[index])
	}
	if s.InverseLabels != nil {
		path, ok := s.InverseLabels[index]
		if ok {
			s.Labels[path] = convertIndex
		}
	}
	s.Conversion[index] = convertIndex
}

func (s *subDAGGenerator) convert() (*Info, error) {
	if s.PrevManifest == nil {
		return nil, fmt.Errorf("no manifest provided")
	}
	if s.RootIndex < 0 || s.RootIndex >= len(s.PrevManifest.Nodes) {
		return nil, ErrIndexOutOfRange
	}

	s.addIndexToSubDAG(s.RootIndex)

	for _, link := range s.PrevManifest.Links {
		fromNode := link[0]
		toNode := link[1]
		currentParent := s.currentParent()
		if fromNode < currentParent {
			continue
		}
		if fromNode > currentParent {
			for {
				currentParent := s.nextParent()
				if currentParent == -1 {
					// have iterated through all the parents!
					// this means our manifest is complete!
					return &Info{
						Manifest: s.Manifest,
						Labels:   s.Labels,
						Sizes:    s.Sizes,
					}, nil
				}
				if currentParent == fromNode {
					// break out of the forloop and continue
					// with the Convert function
					break
				}
				// keep iterating through the Parents array
				continue
			}
		}
		_, fromOk := s.Conversion[fromNode]
		if !fromOk {
			s.addIndexToSubDAG(fromNode)
		}
		_, toOk := s.Conversion[toNode]
		if !toOk {
			s.addIndexToSubDAG(toNode)
		}

		newLink := [2]int{s.Conversion[fromNode], s.Conversion[toNode]}
		s.Manifest.Links = append(s.Manifest.Links, newLink)
		if toNode <= s.LastBranchIndex {
			s.Parents = append(s.Parents, toNode)
		}
	}
	return &Info{Manifest: s.Manifest, Labels: s.Labels, Sizes: s.Sizes}, nil
}

func (s *subDAGGenerator) currentParent() int {
	return s.Parents[s.CurrentParentIndex]
}

func (s *subDAGGenerator) nextParent() int {
	s.CurrentParentIndex++
	if s.CurrentParentIndex >= len(s.Parents) {
		return -1
	}
	return s.currentParent()
}

// InfoAtIndex returns a sub-Info, the DAG, sizes, and labels,
// with the given index as root of the DAG
func (i *Info) InfoAtIndex(idx int) (*Info, error) {
	return newSubDAGGenerator(i, idx).convert()
}

// InfoAtID returns a sub-Info, the DAG, sizes, and labels,
// with the given id as root of the DAG
func (i *Info) InfoAtID(id string) (*Info, error) {
	idx := i.Manifest.IDIndex(id)
	if idx == -1 {
		return nil, ErrIDNotFound
	}
	return i.InfoAtIndex(idx)
}

// InfoAtLabel returns a sub-Info, the DAG, sizes, and labels,
// with the given label as root of the DAG
func (i *Info) InfoAtLabel(label string) (*Info, error) {
	idx, ok := i.Labels[label]
	if !ok {
		return nil, fmt.Errorf("error: label '%s' not found in list of labels", label)
	}
	return i.InfoAtIndex(idx)
}
