package dag

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestInfoAtIndex(t *testing.T) {
	content = 0

	a := newNode(10) // bafkreic75tvwn76in44nsutynrwws3dzyln4eoo5j2i3izzj245cp62x5e
	b := newNode(20) // bafkreidlq2zhh7zu7tqz224aj37vup2xi6w2j2vcf4outqa6klo3pb23jm
	c := newNode(30) // bafkreiguonpdujs6c3xoap2zogfzwxidagoapwfwyupzbwr2mzxoye5lgu
	d := newNode(40) // bafkreicoa5aikyv63ofwbtqfyhpm7y5nc23semewpxqb6zalpzdstne7zy
	e := newNode(50) // bafkreiclej3xpvg5d7dby34ij5egihicwtisdu75gkglbc2vgh6kzwv7ri
	f := newNode(60) // bafkreihpfujh3y33sqv2vudbixsuwddbtipsemt3f2547pwhr5kwjl7dtu
	a.links = []*node{b, c}
	c.links = []*node{d, e}
	d.links = []*node{f}

	ctx := context.Background()
	ng := TestingNodeGetter{[]ipld.Node{a, b, c, d, e, f}}
	di, err := NewInfo(ctx, ng, a.Cid())
	if err != nil {
		t.Fatal(err)
	}

	di.Labels = map[string]int{
		"root": 0,
		"leaf": 5,
	}

	expSubInfoAtC := &Info{
		Manifest: &Manifest{
			Nodes: []cid.Cid{
				mustCid("bafkreiguonpdujs6c3xoap2zogfzwxidagoapwfwyupzbwr2mzxoye5lgu"), // c
				mustCid("bafkreicoa5aikyv63ofwbtqfyhpm7y5nc23semewpxqb6zalpzdstne7zy"), // d
				mustCid("bafkreiclej3xpvg5d7dby34ij5egihicwtisdu75gkglbc2vgh6kzwv7ri"), // e
				mustCid("bafkreihpfujh3y33sqv2vudbixsuwddbtipsemt3f2547pwhr5kwjl7dtu"), // f
			},
			Links: [][2]int{
				{0, 1}, {0, 2}, {1, 3},
			},
		},
		Sizes:  []uint64{30, 40, 50, 60},
		Labels: map[string]int{"leaf": 3},
	}

	expSubInfoAtF := &Info{
		Manifest: &Manifest{
			Nodes: []cid.Cid{
				mustCid("bafkreihpfujh3y33sqv2vudbixsuwddbtipsemt3f2547pwhr5kwjl7dtu"), // f
			},
			Links: [][2]int{},
		},
		Sizes:  []uint64{60},
		Labels: map[string]int{"leaf": 3},
	}

	cases := []struct {
		givenInfo *Info
		index     int
		expInfo   *Info
		err       string
	}{
		{&Info{}, 0, nil, "no manifest provided"},
		{di, -1, nil, ErrIndexOutOfRange.Error()},
		{di, 10, nil, ErrIndexOutOfRange.Error()},
		{di, 1, expSubInfoAtC, ""},
		{di, 5, expSubInfoAtF, ""},
	}

	for i, c := range cases {
		gotInfo, err := c.givenInfo.InfoAtIndex(c.index)
		// TODO (ramfox): this error logic is not pretty, make it prettier
		if err != nil {
			if err.Error() != c.err {
				t.Errorf("case %d, error mismatched. expected %s. got %s", i, c.err, err)
			}
			continue
		}
		if err == nil && c.err != "" {
			t.Errorf("case %d, error mismatched. expected %s. got %s", i, c.err, err)
			continue
		}
		verifyManifest(t, c.expInfo.Manifest, gotInfo.Manifest)

		if len(gotInfo.Sizes) != len(c.expInfo.Sizes) {
			t.Errorf("sizes length mismatch. expected: %d. got: %d", len(c.expInfo.Sizes), len(gotInfo.Sizes))
			continue
		}

		for i, s := range c.expInfo.Sizes {
			if s != gotInfo.Sizes[i] {
				t.Errorf("sizes index %d mismatch. expected: %d, got: %d", i, s, gotInfo.Sizes[i])
			}
		}
	}

}
