<a name="0.1.0"></a>
#  (2019-06-03)

This is the first proper release of `dag`. In preparation for go 1.13, in which `go.mod` files and go modules are the primary way to handle go dependencies, we are going to do an official release of all our modules. This will be version v0.1.0 of `dag`.


### Bug Fixes

* **dag:** Constructor for dag.NodeGetter from a Core API ([ab5ed7d](https://github.com/qri-io/dag/commit/ab5ed7d))
* **dag Fetch:** return early if zero blocks are required for fetch ([ef7c42b](https://github.com/qri-io/dag/commit/ef7c42b))
* **dsync:** fixes for parallelism in dsync ([c748540](https://github.com/qri-io/dag/commit/c748540))
* **Fetch Do:** fetch all the blocks in the manifest, do not diff ([8775080](https://github.com/qri-io/dag/commit/8775080))
* **InfoStore caching:** cache dag.Info when a Receivers receiver completes ([8a5c962](https://github.com/qri-io/dag/commit/8a5c962))
* **subDAGGenerator:** fix bug that returned empty Info when index was a leaf node ([a1f31b0](https://github.com/qri-io/dag/commit/a1f31b0))


### Features

* **dag, dsync:** initial implementations ([7bc921c](https://github.com/qri-io/dag/commit/7bc921c))
* **dag.Info:** add method `AddLabel` to add a label to a dag ([e24a51c](https://github.com/qri-io/dag/commit/e24a51c))
* **dsync http:** support fetching over HTTP ([6c715a6](https://github.com/qri-io/dag/commit/6c715a6))
* **fetch:** fetch DAG from a remote ([24c95f7](https://github.com/qri-io/dag/commit/24c95f7))
* **InfoStore:** intial InfoStore implementation ([5a4ce3e](https://github.com/qri-io/dag/commit/5a4ce3e))
* **ipfs_core_http:** implement skeletion for doing bsync over IPFS HTTP API ([292e80c](https://github.com/qri-io/dag/commit/292e80c))
* **Manifest:** add IDIndex that takes an id and returns the node index ([b116b61](https://github.com/qri-io/dag/commit/b116b61))
* **SubDAG:** given a manifest and an id, get the manifest of the DAG with root id ([29e6b5b](https://github.com/qri-io/dag/commit/29e6b5b))



