package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
)

var (
	cmdAddr = "http://localhost:2502"
	noPin   = false
)

// root command
var root = &cobra.Command{
	Use:   "dsync",
	Short: "dsync is like rsync for Merkle-DAGs",
	Long: `Requires an IPFS plugin. More details:
https://github.com/qri-io/dag`,
}

// push
// curl -X POST http://$DSYNC_ADDR/push?cid=$CID&addr=$ADDR&pin=$PIN
var push = &cobra.Command{
	Use:   "push",
	Short: "copy local [cid] to remote [peerId]",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		cid := args[0]
		addr := args[1]
		endpoint := fmt.Sprintf("/push?cid=%s&addr=%s&pin=%t", cid, addr, !noPin)

		_, err := doRemoteHTTPReq("POST", endpoint)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("pushed cid %s to:\n\t%s\n", cid, addr)
	},
}

// pull
// curl -X GET http://$DSYNC_ADDR/pull?cid=$CID&addr=$ADDR&pin=$PIN
var pull = &cobra.Command{
	Use:   "pull",
	Short: "copy remote [cid] from [peerId] to local repo",
	Run: func(cmd *cobra.Command, args []string) {
		cid := args[0]
		addr := args[1]

		endpoint := fmt.Sprintf("/pull?cid=%s&addr=%s&pin=%t", cid, addr, !noPin)
		_, err := doRemoteHTTPReq("GET", endpoint)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// fmt.Println(res)
		fmt.Printf("pulled cid %s from:\n\t%s\n", cid, addr)
	},
}

// add an address to your allow list:
// curl -X GET http://$DSYNC_ADDR/acl?addr=$ADDR
var allow = &cobra.Command{
	Use:  "allow",
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// TODO (b5) - expand API to allow multiple allows at once
		addr := args[0]
		endpoint := fmt.Sprintf("/acl?&addr=%s", addr)
		res, err := doRemoteHTTPReq("POST", endpoint)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(res)
	},
}

// remove an address from your allow list:
// curl -X DELETE http://$DSYNC_ADDR/acl?addr=$ADDR
var deny = &cobra.Command{
	Use: "deny",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO (b5) - expand API to allow multiple allows at once
		addr := args[0]
		endpoint := fmt.Sprintf("/acl?&addr=%s", addr)
		res, err := doRemoteHTTPReq("DELETE", endpoint)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(res)
	},
}

func init() {
	push.Flags().BoolVar(&noPin, "no-pin", noPin, "skip pinning")
	pull.Flags().BoolVar(&noPin, "no-pin", noPin, "skip pinning")

	root.PersistentFlags().StringVar(&cmdAddr, "command-address", cmdAddr, "address to issue requests that control local dsync")
	root.AddCommand(push, pull, allow, deny)
}

func main() {
	if err := root.Execute(); err != nil {
		fmt.Println(err)
	}
}

func doRemoteHTTPReq(method, endpoint string) (resMsg string, err error) {
	url := fmt.Sprintf("%s%s", cmdAddr, endpoint)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	resBytes, err := ioutil.ReadAll(res.Body)
	if resBytes == nil {
		return
	}

	resMsg = string(resBytes)
	return
}
