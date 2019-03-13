package dag

import (
  "testing"
  "context"
    ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
)

func TestSubInfoAtIndex(t *testing.T) {
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
  blankInfo := &Info{}
  _, err = blankInfo.SubInfoAtIndex(0)
  if err == nil || err.Error() != "no manifest provided" {
    t.Errorf("empty Info error mismatch, expected 'no manifest provided', got: '%s'", err)
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

  _, err = di.SubInfoAtIndex(-1)
  if err == nil || err.Error() != ErrIndexOutOfRange.Error() {
    t.Errorf("error mismatch. expected '%s', got: '%s'", ErrIndexOutOfRange, err)
  }

  _, err = di.SubInfoAtIndex(10)
  if err == nil || err.Error() != ErrIndexOutOfRange.Error() {
    t.Errorf("error mismatch. expected '%s', got: '%s'", ErrIndexOutOfRange, err)
  }

  SubinfoAtC, err := di.SubInfoAtIndex(1)
  if err != nil {
    t.Errorf("unexpected error: %s", err)
    return
  }

  verifyManifest(t, expSubInfoAtC.Manifest, SubinfoAtC.Manifest)

  if len(expSubInfoAtC.Sizes) != len(SubinfoAtC.Sizes) {
    t.Errorf("sizes length mismatch. expected: %d. got: %d", len(expSubInfoAtC.Sizes), len(SubinfoAtC.Sizes))
    return
  }

  for i, s := range expSubInfoAtC.Sizes {
    if s != SubinfoAtC.Sizes[i] {
      t.Errorf("sizes index %d mismatch. expected: %d, got: %d", i, s, SubinfoAtC.Sizes[i])
    }
  }
}