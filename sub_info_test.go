package dag

import (
	"context"
	ipld "github.com/ipfs/go-ipld-format"
	"testing"
)

func TestInfoAtIndex(t *testing.T) {
	content = 0

	a := newNode(10) // zb2rhd6jTUt94FLVLjrCJ6Wy3NMDxm2sDuwArDfuDaNeHGRi8
	b := newNode(20) // zb2rhdt1wgqfpzMgYf7mefxCWToqUTTyriWA1ctNxmy5WojSz
	c := newNode(30) // zb2rhkwbf5N999rJcRX3D89PVDibZXnctArZFkap4CB36QcAQ
	d := newNode(40) // zb2rhbtsQanqdtuvSceyeKUcT4ao1ge7HULRuRbueGjznWsDP
	e := newNode(50) // zb2rhbhaFdd82br6cP9uUjxQxUyrMFwR3K6uYt6YvUxJtgpSV
	f := newNode(60) // zb2rhnjvVfrzHtyeBcrCt3QUshMoYvEaxPXDykT4MyWvTCKV6
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
			Nodes: []string{
				"zb2rhkwbf5N999rJcRX3D89PVDibZXnctArZFkap4CB36QcAQ", // c
				"zb2rhbtsQanqdtuvSceyeKUcT4ao1ge7HULRuRbueGjznWsDP", // d
				"zb2rhbhaFdd82br6cP9uUjxQxUyrMFwR3K6uYt6YvUxJtgpSV", // e
				"zb2rhnjvVfrzHtyeBcrCt3QUshMoYvEaxPXDykT4MyWvTCKV6", // f
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
			Nodes: []string{
				"zb2rhnjvVfrzHtyeBcrCt3QUshMoYvEaxPXDykT4MyWvTCKV6", // f
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
