// DsyncPlugin is an ipfs deamon plugin for embedding dsync functionality
// directly into IPFS
// https://github.com/ipfs/go-ipfs-example-plugin
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	ipfscoreapi "github.com/ipfs/go-ipfs/core/coreapi"
	plugin "github.com/ipfs/go-ipfs/plugin"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	dag "github.com/qri-io/dag"
	dsync "github.com/qri-io/dag/dsync"
	golog "github.com/ipfs/go-log"
)


var log = golog.Logger("dsync-plugin")

// Plugins is an exported list of plugins that will be loaded by go-ipfs
var Plugins = []plugin.Plugin{
	NewDsyncPlugin(),
}

// DsyncPlugin is exported struct IPFS will load & work with
type DsyncPlugin struct {
	configPath string
	host       *dsync.Dsync
	// log level, defaults to "info"
	LogLevel string
	// Address dsync will listen on for commands. This should be local only
	HTTPCommandsAddr string
	// Address dsync will listen on for performing dsync
	HTTPRemoteAddr string
	// Maximum size of DAG dsync will accept
	MaxDAGSize int
	// allow-list of peerIDs to accept DAG pushes
	AllowAddrs []string
}

// NewDsyncPlugin creates a DsyncPlugin with some sensible defaults
// at least one address will need to be explicitly added to the AllowAddrs
// list before anyone can push to this node
func NewDsyncPlugin() *DsyncPlugin {
	cfgPath, err := configPath()
	if err != nil {
		panic(err)
	}

	return &DsyncPlugin{
		configPath:       cfgPath,
		LogLevel: "info",
		HTTPRemoteAddr:   ":2503",
		HTTPCommandsAddr: "127.0.0.1:2502",
		MaxDAGSize:       -1, // default to allowing any size of DAG
	}
}

// assert at compile time that DsyncPlugin support the PluginDaemon interface
var _ plugin.PluginDaemon = (*DsyncPlugin)(nil)

// Name returns the plugin's name, satisfying the plugin.Plugin interface.
func (*DsyncPlugin) Name() string {
	return "dsync"
}

// Version returns the plugin's version, satisfying the plugin.Plugin interface.
func (*DsyncPlugin) Version() string {
	return "0.0.1"
}

// Init initializes plugin, loading configuration details
func (p *DsyncPlugin) Init() error {
	if err := p.LoadConfig(); err != nil {
		return err
	}
	golog.SetLogLevel("dsync-plugin", p.LogLevel)
	return nil
}

// Start the plugin
func (p *DsyncPlugin) Start(capi coreiface.CoreAPI) error {
	// unfortunately we need to break coreAPI encapsulation here, which requires
	// forking IPFS itself. dsync requires registering a new libp2p protocol
	// which requires access to the underlying libp2p.Host
	// dsync one part ipfs plugin, and one part "libp2p plugin".
	// Since the notion of a libp2p plugin doesn't currently exist, this is worth
	// chatting over with the ipfs & libp2p teams
	ipfsNode, ok := capi.(*ipfscoreapi.CoreAPI)
	if !ok {
		return fmt.Errorf("expected coreapi to be a CoreAPI instance")
	}

	// check for a p2p host value:
	libp2pHost := ipfsNode.Host()
	if libp2pHost == nil {
		return fmt.Errorf("no p2p host present, skipping dsync registration")
	}

	// create an ipld NodeGetter that doesn't perform any network requests
	// without creating a local-only node getter, the act of asking for
	// information about a dag will fire off network requests that try to
	// resolve the dag, which kinda defeats the whole reason for
	lng, err := dsync.NewLocalNodeGetter(capi)
	if err != nil {
		return err
	}

	p.host, err = dsync.New(lng, capi.Block(), func(cfg *dsync.Config) {
		// if supplied a libp2phost, dsync will register a /dsync protocol
		// when StartRemote is called
		cfg.Libp2pHost = libp2pHost

		// address dsync will listen on.
		// NOTE: It would ba *amazing* if we could avoid consuming another
		// port and instead attach these http handlers to the  (presumably already
		// running) http API server. This would also help with security concerns
		// by matching configuration vis-a-vis external HTTP API accessibility
		cfg.HTTPRemoteAddress = p.HTTPRemoteAddr

		// we MUST override the PreCheck function. In this example we're making sure
		// no one sends us a bad hash:
		cfg.PreCheck = p.pushPreCheck

		// in order for remotes to allow pinning, dsync must be provided a PinAPI:
		cfg.PinAPI = capi.Pin()
	})
	if err != nil {
		return err
	}

	// start listening for remote pushes & pulls. We bind this context to the
	// one provided by ipfs.
	if err = p.host.StartRemote(ipfsNode.Context()); err != nil {
		return err
	}

	go p.listenLocalCommads()

	fmt.Printf("dsync plugin started. listening for commands: %s\n", p.HTTPCommandsAddr)
	return nil
}

// Close the plugin
func (*DsyncPlugin) Close() error {
	return nil
}

// pushPreCheck enforces the allow list & max size
func (p *DsyncPlugin) pushPreCheck(ctx context.Context, info dag.Info) error {
	if info.Manifest.Nodes[0] == "BadHash" {
		return fmt.Errorf("rejected for secret reasons")
	}
	log.Info("accepting push.\n\tcid: %s", info.Manifest.RootCID())
	return nil
}

// LoadConfig loads configuration details
func (p *DsyncPlugin) LoadConfig() error {
	cfg := &DsyncPlugin{}

	// attempt to read from enviornment, nice for docker-type settings
	envConfigJSON := os.Getenv("DSYNC_CONFIG")
	if envConfigJSON != "" {
		if err := json.Unmarshal([]byte(envConfigJSON), cfg); err != nil {
			return fmt.Errorf("reading configuration JSON data from enviornment: %s", err.Error())
		}
		*p = *cfg
		return nil
	}

	path, err := configPath()
	if err != nil {
		return err
	}

	cfgFile, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := json.NewDecoder(cfgFile).Decode(cfg); err != nil {
		return err
	}

	*p = *cfg
	p.configPath = path
	return nil
}

// WriteConfig records configuration details
func (p *DsyncPlugin) WriteConfig() error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(p.configPath, data, os.ModePerm)
}

// AllowAddr adds a record to the list of addresses that are allowed to
// push & pull from the local dsync host
func (p *DsyncPlugin) AllowAddr(addrStr string) error {
	for _, a := range p.AllowAddrs {
		if addrStr == a {
			return nil
		}
	}
	p.AllowAddrs = append(p.AllowAddrs, addrStr)
	return p.WriteConfig()
}

// DenyAddr removes a record from the list of addresses that are allowed to
// push & pull from the local dsync host
func (p *DsyncPlugin) DenyAddr(addrStr string) error {
	for i, a := range p.AllowAddrs {
		if addrStr == a {
			if i+1 == len(p.AllowAddrs) {
				p.AllowAddrs = p.AllowAddrs[:i]
			} else {
				p.AllowAddrs = append(p.AllowAddrs[:i], p.AllowAddrs[i+1:]...)
			}
			return p.WriteConfig()
		}
	}
	return nil
}

func configPath() (string, error) {
	ipfsRepoPath, err := ipfsRepoFilepath()
	if err != nil {
		return "", err
	}

	return filepath.Join(ipfsRepoPath, "dsync.config.json"), nil
}

// TODO (b5) - this will (probably) fail to pick up on any `--config` flags
// provided to the ipfs command, resulting in configuration either not being
// loaded, or stored in an unexpected location
//
// it'd be nice if this was an exported function from ipfs land:
// https://github.com/ipfs/go-ipfs/blob/1c6043d67ca1301573f0650cf0a227cd3386bb4f/cmd/ipfs/main.go#L301
func ipfsRepoFilepath() (string, error) {
	return fsrepo.BestKnownPath()
}

// MoveParams is a generic push/pull params struct
type MoveParams struct {
	Cid, Addr string
	Pin       bool
}

func (p *DsyncPlugin) listenLocalCommads() error {
	m := http.NewServeMux()
	m.Handle("/push", newPushHandler(p.host))
	m.Handle("/pull", newPushHandler(p.host))
	m.Handle("/acl", newACLHandler(p))
	return http.ListenAndServe(p.HTTPCommandsAddr, m)
}

func newPushHandler(dsyncHost *dsync.Dsync) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			p := MoveParams{
				Cid:  r.FormValue("cid"),
				Addr: r.FormValue("addr"),
				Pin:  r.FormValue("pin") == "true",
			}
			log.Infof("performing push:\n\tcid: %s\n\tremote: %s\n\tpin: %t\n", p.Cid, p.Addr, p.Pin)

			push, err := dsyncHost.NewPush(p.Cid, p.Addr, p.Pin)
			if err != nil {
				fmt.Printf("error creating push: %s\n", err.Error())
				w.Write([]byte(err.Error()))
				return
			}

			if err = push.Do(r.Context()); err != nil {
				fmt.Printf("push error: %s\n", err.Error())
				w.Write([]byte(err.Error()))
				return
			}

			fmt.Println("push complete")

			data, err := json.Marshal(p)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}

			w.Header().Add("Content-Type", "application/json")
			w.Write(data)
		}
	})
}

func newPullHandler(dsyncHost *dsync.Dsync) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			p := MoveParams{
				Cid:  r.FormValue("cid"),
				Addr: r.FormValue("addr"),
				Pin:  r.FormValue("pin") == "true",
			}
			fmt.Printf("performing pull:\n\tcid: %s\n\tremote: %s\n\tpin: %t\n", p.Cid, p.Addr, p.Pin)

			pull, err := dsyncHost.NewPull(p.Cid, p.Addr)
			if err != nil {
				fmt.Printf("error creating pull: %s\n", err.Error())
				w.Write([]byte(err.Error()))
				return
			}

			if err = pull.Do(r.Context()); err != nil {
				fmt.Printf("pull error: %s\n", err.Error())
				w.Write([]byte(err.Error()))
				return
			}

			data, err := json.Marshal(p)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}

			w.Header().Add("Content-Type", "application/json")
			w.Write(data)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func newACLHandler(p *DsyncPlugin) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			addrStr := r.FormValue("addr")
			log.Infof("allowing address: %s", addrStr)
			if err := p.AllowAddr(addrStr); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}
		case "DELETE":
			addrStr := r.FormValue("addr")
			log.Infof("removing allow address: %s", addrStr)
			if err := p.DenyAddr(addrStr); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}
