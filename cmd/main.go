package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/knadh/koanf/v2"
	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tarungka/wire/internal/cluster"
	"github.com/tarungka/wire/internal/cmd"
	httpd "github.com/tarungka/wire/internal/http"
	"github.com/tarungka/wire/internal/store"
	"github.com/tarungka/wire/internal/tcp"
	pipeline "github.com/tarungka/wire/pipeline"
	"github.com/tarungka/wire/server"
)

var (
	buildString = "unknown"
	ko          = koanf.New(".")
)

func main() {

	var logger zerolog.Logger

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	// zerolog.SetGlobalLevel(zerolog.InfoLevel)
	// logs will be written to both server.log and stdout
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open log file")
	}
	defer logFile.Close()

	// Create a multi-writer to write to both the console and the log file
	multi := zerolog.MultiLevelWriter(os.Stdout, logFile)

	// Set up zerolog to write to the multi-writer
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	initFlags(ko)

	// Set up zerolog for development mode (human-readable logs)
	isDevelopment := ko.Bool("debug")

	if isDevelopment {
		consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339,
			FormatLevel: func(i interface{}) string {
				return strings.ToUpper(fmt.Sprintf("[%5s]", i))
			},
			FormatMessage: func(i interface{}) string {
				return fmt.Sprintf("| %s |", i)
			},
			FormatCaller: func(i interface{}) string {
				return filepath.Base(fmt.Sprintf("%s", i))
			},
			PartsExclude: []string{
				zerolog.TimestampFieldName,
			}}
		// Use multi-writer for file and readable console output
		multiDev := zerolog.MultiLevelWriter(consoleWriter, logFile)
		logger = zerolog.New(multiDev).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()
	} else {
		// Production: Use default JSON format for logs
	}

	// Assign the logger as the global logger
	log.Logger = logger

	if isDevelopment {
		log.Debug().Msgf("The process ID is: %v", os.Getpid())
	}

	if ko.Bool("version") {
		fmt.Println(buildString)
		os.Exit(0)
	} else {
		log.Info().Str("build:", buildString).Msgf("Build Version: %s", buildString)
	}

	log.Info().Msg("Starting the application...")

	raftAddress := ko.String("raft_addr")
	muxListener, err := net.Listen("tcp", raftAddress)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to listen on %s: %s", raftAddress, err.Error())
	}
	mux, err := startNodeMux(ko, muxListener)
	if err != nil {
		log.Fatal().Msgf("failed to start node mux: %s", err.Error())
	}

	nodeVerifyServername := ko.String("node_verify_server_name")
	noNodeVerify := ko.Bool("no_node_verify")
	// Raft internode layer
	raftLn := mux.Listen(cluster.MuxRaftHeader)
	raftDialer, err := cluster.CreateRaftDialer("", "", "", nodeVerifyServername, noNodeVerify)
	if err != nil {
		log.Fatal().Msgf("failed to create Raft dialer: %s", err.Error())
	}
	raftTn := tcp.NewLayer(raftLn, raftDialer)

	log.Debug().Msgf("A raft layer is ready, will use it: %v", raftTn)

	// Create the store
	str, err := createStore(ko, raftTn)
	if err != nil {
		log.Fatal().Msgf("failed to create store: %s", err.Error())
	}
	log.Debug().Msgf("The store is:", str)

	// Create cluster service now, so nodes will be able to learn information about each other.
	clstrServ, err := clusterService(ko, mux.Listen(cluster.MuxClusterHeader), str)
	if err != nil {
		log.Fatal().Msgf("failed to create cluster service: %s", err.Error())
	}
	log.Debug().Msgf("Created the cluster service: %v", clstrServ)

	clstrClient, err := createClusterClient(ko, clstrServ)
	if err != nil {
		log.Fatal().Msgf("failed to create cluster client: %s", err.Error())
	}
	log.Debug().Msgf("Created the cluster client: %v", clstrClient)
	httpServ, err := startHTTPService(ko, str, clstrClient)
	if err != nil {
		log.Fatal().Msgf("failed to start HTTP server: %s", err.Error())
	}
	log.Debug().Msgf("Started the HTTP service!", httpServ)


	// Now, open the store
	if err := str.Open(); err != nil {
		log.Fatal().Msgf("failed to open store: %s", err.Error())
	}

	// Creating a main context; will need to move this code up
	// mainCtx := context.Background()

	// // Create the cluster!
	// nodes, err := str.Nodes()
	// if err != nil {
	// 	log.Fatal().Msgf("failed to get nodes %s", err.Error())
	// }
	// log.Debug().Msgf("The number of nodes is: %s", nodes)

	// // fmt.Printf("%v %v\n", mainCtx, nodes)
	// if err := createCluster(mainCtx, ko, len(nodes) > 0, clstrClient, str, httpServ, nil); err != nil {
	// 	log.Fatal().Msgf("clustering failure: %s", err.Error())
	// }

	// This way the command line arguments are overridden by the remote/other configs
	if ko.Bool("override") {
		if initError := initConfig(ko); initError != nil {
			log.Err(initError).Msg("Error when initializing the config!")
		}
	}

	done := make(chan interface{}, 1)

	var wg sync.WaitGroup

	// Run the web server
	go func(ko *koanf.Koanf) {
		log.Info().Msg("Starting the web server...")
		server.Init(ko)
		// wg is used only for creating a new pipeline
		server.Run(done, &wg, ko)
	}(ko)

	// Start pipelines that have been specified in the config file
	// var dataPipelineConfig pipeline.PipelineDataObject
	dataPipelineConfig := pipeline.GetPipelineInstance()

	allSourcesConfig, allSinksConfig, err := dataPipelineConfig.ParseConfig(ko)
	if err != nil {
		log.Err(err).Msg("Error when reading config")
	}

	for _, sourceConfig := range allSourcesConfig {
		dataPipelineConfig.AddSource(sourceConfig)
	}
	for _, sinkConfig := range allSinksConfig {
		dataPipelineConfig.AddSink(sinkConfig)
	}

	mappedDataPipelines, exists := dataPipelineConfig.GetMappedPipelines()
	if !exists {
		log.Debug().Msg("No data pipelines exist")
	}

	// Run each pipeline
	for pipelineKey, eachDataPipeline := range mappedDataPipelines {
		log.Debug().Msgf("Key: %s | Value: %v", pipelineKey, eachDataPipeline)
		newPipeline := pipeline.NewDataPipeline(eachDataPipeline.Source, eachDataPipeline.Sink)
		pipelineString, err := newPipeline.Show()
		if err != nil {
			log.Err(err).Send()
		}
		log.Debug().Msgf("Creating and running pipeline: %s", pipelineString)

		wg.Add(1)
		go newPipeline.Run(done, &wg)
	}

	// Wait for an interrupt signal (ctrl+c)
	signalChannel := make(chan os.Signal, 1)
	// TODO: Catch SIGTERM and handle it
	signal.Notify(signalChannel, os.Interrupt)
	<-signalChannel // Blocks until an interrupt signal is received

	log.Info().Msg("Process interrupted, shutting down...")

	// Close the done channel to signal all goroutines to exit
	close(done)

	// For for graceful shutdown
	wg.Wait()
}

// startNodeMux starts the TCP mux on the given listener, which should be already
// bound to the relevant interface.
func startNodeMux(ko *koanf.Koanf, ln net.Listener) (*tcp.Mux, error) {
	raftAddress := ko.String("raft_addr")
	var err error
	adv := tcp.NameAddress{
		Address: raftAddress,
	}

	var mux *tcp.Mux
	if ko.String("NodeX509Cert") != "" {
		// TODO: Implement this later
		// var b strings.Builder
		// b.WriteString(fmt.Sprintf("enabling node-to-node encryption with cert: %s, key: %s",
		// 	cfg.NodeX509Cert, cfg.NodeX509Key))
		// if cfg.NodeX509CACert != "" {
		// 	b.WriteString(fmt.Sprintf(", CA cert %s", cfg.NodeX509CACert))
		// }
		// if cfg.NodeVerifyClient {
		// 	b.WriteString(", mutual TLS enabled")
		// } else {
		// 	b.WriteString(", mutual TLS disabled")
		// }
		// log.Println(b.String())
		// mux, err = tcp.NewTLSMux(ln, adv, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
		// 	cfg.NoNodeVerify, cfg.NodeVerifyClient)
	} else {
		mux, err = tcp.NewMux(ln, adv)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux.Serve()
	return mux, nil
}

func clusterService(ko *koanf.Koanf, ln net.Listener, mgr cluster.Manager) (*cluster.Service, error) {
	c := cluster.New(ln, mgr)
	apiAddress := ko.String("http_adv")
	c.SetAPIAddr(apiAddress)
	// TODO: support HTTP over SSL
	c.EnableHTTPS(false) // Conditions met for an HTTPS API
	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

func createClusterClient(ko *koanf.Koanf, clstr *cluster.Service) (*cluster.Client, error) {
	var dialerTLSConfig *tls.Config
	// TODO: Dialer over SSL
	// var err error
	// if cfg.NodeX509Cert != "" || cfg.NodeX509CACert != "" {
	// 	dialerTLSConfig, err = rtls.CreateClientConfig(cfg.NodeX509Cert, cfg.NodeX509Key,
	// 		cfg.NodeX509CACert, cfg.NodeVerifyServerName, cfg.NoNodeVerify)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to create TLS config for cluster dialer: %s", err.Error())
	// 	}
	// }

	// clusterConnectTimeout := ko.Int64("cluster_connect_timeout")
	raftAddress := ko.String("raft_addr")
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, dialerTLSConfig)
	clstrClient := cluster.NewClient(clstrDialer, 10*time.Second)
	if err := clstrClient.SetLocal(raftAddress, clstr); err != nil {
		return nil, fmt.Errorf("failed to set cluster client local parameters: %s", err.Error())
	}
	return clstrClient, nil
}

func createStore(ko *koanf.Koanf, ln *tcp.Layer) (*store.Store, error) {

	str := store.New(ln, &store.Config{
		Dir: ko.String("raft_dir"),
		ID:  ko.String("node_id"),
	})

	// Set optional parameters on store.
	str.RaftLogLevel = ko.String("RaftLogLevel")
	str.ShutdownOnRemove = ko.Bool("RaftShutdownOnRemove")
	str.SnapshotThreshold = uint64(ko.Int64("RaftSnapThreshold"))
	// str.SnapshotThresholdWALSize = uint64(ko.Int64("RaftSnapThresholdWALSize"))
	str.SnapshotInterval = time.Duration(ko.Int64("RaftSnapInterval")) * time.Second
	str.LeaderLeaseTimeout = time.Duration(ko.Int64("RaftLeaderLeaseTimeout")) * time.Second
	str.HeartbeatTimeout = time.Duration(ko.Int64("RaftHeartbeatTimeout")) * time.Second
	str.ElectionTimeout = time.Duration(ko.Int64("RaftElectionTimeout")) * time.Second
	str.ApplyTimeout = time.Duration(ko.Int64("RaftApplyTimeout")) * time.Second
	str.BootstrapExpect = ko.Int("BootstrapExpect")
	str.ReapTimeout = time.Duration(ko.Int64("RaftReapNodeTimeout")) * time.Second
	str.ReapReadOnlyTimeout = time.Duration(ko.Int64("RaftReapReadOnlyNodeTimeout")) * time.Second
	str.AutoVacInterval = time.Duration(ko.Int64("AutoVacInterval")) * time.Second
	str.AutoOptimizeInterval = time.Duration(ko.Int64("AutoOptimizeInterval")) * time.Second

	if store.IsNewNode(ko.String("raft_dir")) {
		log.Printf("no preexisting node state detected in %s, node may be bootstrapping", ko.String("raft_dir"))
	} else {
		log.Printf("preexisting node state detected in %s", ko.String("DataPath"))
	}

	return str, nil
}

// func startHTTPService(ko *koanf.Koanf, str *store.Store, cltr *cluster.Client, credStr *auth.CredentialsStore) (string, error) {
func startHTTPService(ko *koanf.Koanf, str *store.Store, cltr *cluster.Client) (*httpd.Service, error) {

	defer func() {
		log.Debug().Msg("Http service started/failed!")
	}()

	// Create HTTP server and load authentication information.
	s := httpd.New(ko.String("http_addr"), str, cltr, nil)

	log.Debug().Msg("Started the HTTP service!")
	// s.CACertFile = cfg.HTTPx509CACert
	// s.CertFile = cfg.HTTPx509Cert
	// s.KeyFile = cfg.HTTPx509Key
	// s.ClientVerify = cfg.HTTPVerifyClient
	// s.DefaultQueueCap = cfg.WriteQueueCap
	// s.DefaultQueueBatchSz = cfg.WriteQueueBatchSz
	// s.DefaultQueueTimeout = cfg.WriteQueueTimeout
	// s.DefaultQueueTx = cfg.WriteQueueTx
	s.BuildInfo = map[string]interface{}{
		"commit":             cmd.Commit,
		"branch":             cmd.Branch,
		"version":            cmd.Version,
		"compiler_toolchain": runtime.Compiler,
		"compiler_command":   cmd.CompilerCommand,
		"build_time":         cmd.Buildtime,
	}
	// s.SetAllowOrigin(cfg.HTTPAllowOrigin)
	return s, s.Start()
}

// TODO: This code needs major rework, will work on this later
func createCluster(ctx context.Context, ko *koanf.Koanf, hasPeers bool, client *cluster.Client, str *store.Store,
	httpServ *httpd.Service, credStr *auth.CredentialsStore) error {
	// joins := cfg.JoinAddresses()
	joins, err := joinAddresses(ko.String("join"))
	if err != nil {
		log.Fatal().Msgf("Invalid input for join: %s", ko.String("join"))
	}
	if err := networkCheckJoinAddrs(joins); err != nil {
		return err
	}
	if joins == nil && ko.String("disco_mode") == "" && !hasPeers {
		if ko.Bool("raft-non-voter") {
			return fmt.Errorf("cannot create a new non-voting node without joining it to an existing cluster")
		}

		// Brand new node, told to bootstrap itself. So do it.
		log.Info().Msg("bootstrapping single new node")
		if err := str.Bootstrap(store.NewServer(str.ID(), ko.String("raft_addr"), true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	// Prepare definition of being part of a cluster.
	bootDoneFn := func() bool {
		leader, _ := str.LeaderAddr()
		return leader != ""
	}
	clusterSuf := cluster.VoterSuffrage(!ko.Bool("raft-non-voter"))

	joiner := cluster.NewJoiner(client, ko.Int("join-attempts"), ko.Duration("join-interval"))
	joiner.SetCredentials(cluster.CredentialsFor(credStr, ko.String("join-as")))
	if joins != nil && ko.Int("bootstrap-expect") == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(ctx, joins, str.ID(), ko.String("raft_addr"), clusterSuf)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		log.Info().Msgf("successfully joined cluster at %v", j)
		return nil
	}

	if joins != nil && ko.Int("bootstrap-expect") > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := cluster.NewBootstrapper(cluster.NewAddressProviderString(joins), client)
		bs.SetCredentials(cluster.CredentialsFor(credStr, ko.String("join-as")))
		return bs.Boot(ctx, str.ID(), ko.String("raft_addr"), clusterSuf, bootDoneFn, ko.Duration("bootstrap-expect-timeout"))
	}

	if ko.String("disco_mode") == "" {
		// No more clustering techniques to try. Node will just sit, probably using
		// existing Raft state.
		return nil
	}

	// DNS-based discovery requested. It's OK to proceed with this even if this node
	// is already part of a cluster. Re-joining and re-notifying other nodes will be
	// ignored when the node is already part of the cluster.
	log.Printf("discovery mode: %s", ko.String("disco_mode"))
	switch ko.String("disco_mode") {
	// TODO: will impl this later
	// case DiscoModeDNS, DiscoModeDNSSRV:
	// 	rc := cfg.DiscoConfigReader()
	// 	defer func() {
	// 		if rc != nil {
	// 			rc.Close()
	// 		}
	// 	}()

	// 	var provider interface {
	// 		cluster.AddressProvider
	// 		httpd.StatusReporter
	// 	}
	// 	if cfg.DiscoMode == DiscoModeDNS {
	// 		dnsCfg, err := dns.NewConfigFromReader(rc)
	// 		if err != nil {
	// 			return fmt.Errorf("error reading DNS configuration: %s", err.Error())
	// 		}
	// 		provider = dns.NewWithPort(dnsCfg, cfg.RaftPort())

	// 	} else {
	// 		dnssrvCfg, err := dnssrv.NewConfigFromReader(rc)
	// 		if err != nil {
	// 			return fmt.Errorf("error reading DNS configuration: %s", err.Error())
	// 		}
	// 		provider = dnssrv.New(dnssrvCfg)
	// 	}

	// 	bs := cluster.NewBootstrapper(provider, client)
	// 	bs.SetCredentials(cluster.CredentialsFor(credStr, cfg.JoinAs))
	// 	httpServ.RegisterStatus("disco", provider)
	// 	return bs.Boot(ctx, str.ID(), ko.String("raft_addr"), clusterSuf, bootDoneFn, cfg.BootstrapExpectTimeout)

	// case DiscoModeEtcdKV, DiscoModeConsulKV:
	// 	discoService, err := createDiscoService(cfg, str)
	// 	if err != nil {
	// 		return fmt.Errorf("failed to start discovery service: %s", err.Error())
	// 	}
	// 	// Safe to start reporting before doing registration. If the node hasn't bootstrapped
	// 	// yet, or isn't leader, reporting will just be a no-op until something changes.
	// 	go discoService.StartReporting(cfg.NodeID, cfg.HTTPURL(), ko.String("raft_addr"))
	// 	httpServ.RegisterStatus("disco", discoService)

	// 	if hasPeers {
	// 		log.Printf("preexisting node configuration detected, not registering with discovery service")
	// 		return nil
	// 	}
	// 	log.Info().Msg("no preexisting nodes, registering with discovery service")

	// 	leader, addr, err := discoService.Register(str.ID(), cfg.HTTPURL(), ko.String("raft_addr"))
	// 	if err != nil {
	// 		return fmt.Errorf("failed to register with discovery service: %s", err.Error())
	// 	}
	// 	if leader {
	// 		log.Info().Msg("node registered as leader using discovery service")
	// 		if err := str.Bootstrap(store.NewServer(str.ID(), str.Addr(), true)); err != nil {
	// 			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
	// 		}
	// 	} else {
	// 		for {
	// 			log.Printf("discovery service returned %s as join address", addr)
	// 			if j, err := joiner.Do(ctx, []string{addr}, str.ID(), ko.String("raft_addr"), clusterSuf); err != nil {
	// 				log.Printf("failed to join cluster at %s: %s", addr, err.Error())

	// 				time.Sleep(time.Second)
	// 				_, addr, err = discoService.Register(str.ID(), cfg.HTTPURL(), ko.String("raft_addr"))
	// 				if err != nil {
	// 					log.Printf("failed to get updated leader: %s", err.Error())
	// 				}
	// 				continue
	// 			} else {
	// 				log.Info().Msg("successfully joined cluster at", j)
	// 				break
	// 			}
	// 		}
	// 	}

	default:
		return fmt.Errorf("invalid disco mode %s", ko.String("disco_mode"))
	}
	return nil
}

func joinAddresses(joinAddrs string) ([]string, error) {
	addrs := strings.Split(joinAddrs, ",")
	for i := range addrs {
		if _, _, err := net.SplitHostPort(addrs[i]); err != nil {
			return nil, fmt.Errorf("%s is an invalid join address", addrs[i])

		}
	}
	return strings.Split(joinAddrs, ","), nil
}

func networkCheckJoinAddrs(joinAddrs []string) error {
	if len(joinAddrs) > 0 {
		log.Debug().Msg("checking that supplied join addresses don't serve HTTP(S)")
		if addr, ok := httpd.AnyServingHTTP(joinAddrs); ok {
			return fmt.Errorf("join address %s appears to be serving HTTP when it should be Raft", addr)
		}
	}
	return nil
}
