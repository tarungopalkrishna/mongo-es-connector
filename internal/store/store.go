package store

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/tarungka/wire/internal/command"
	"github.com/tarungka/wire/internal/command/proto"
	commandProto "github.com/tarungka/wire/internal/command/proto"
	"github.com/tarungka/wire/internal/rsync"
	"github.com/tarungka/wire/internal/snapshot"
	"github.com/tarungka/wire/internal/utils"

	rlog "github.com/tarungka/wire/internal/log"
)

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrOpen is returned when a Store is already open.
	ErrOpen = errors.New("store already open")

	// ErrNotReady is returned when a Store is not ready to accept requests.
	ErrNotReady = errors.New("store not ready")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrNotSingleNode is returned when a node attempts to execute a single-node
	// only operation.
	ErrNotSingleNode = errors.New("not single-node")

	// ErrStaleRead is returned if the executing the query would violate the
	// requested freshness.
	ErrStaleRead = errors.New("stale read")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrWaitForRemovalTimeout is returned when the Store does not confirm removal
	// of a node within the specified time.
	ErrWaitForRemovalTimeout = errors.New("timeout waiting for node removal confirmation")

	// ErrWaitForLeaderTimeout is returned when the Store cannot determine the leader
	// within the specified time.
	ErrWaitForLeaderTimeout = errors.New("timeout waiting for leader")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")

	// ErrInvalidVacuumFormat is returned when the requested backup format is not
	// compatible with vacuum.
	ErrInvalidVacuum = errors.New("invalid vacuum")

	// ErrLoadInProgress is returned when a load is already in progress and the
	// requested operation cannot be performed.
	ErrLoadInProgress = errors.New("load in progress")

	// ErrNotImplemented when there is no implementation of the function
	// will only exits until this application in under development
	ErrNotImplemented = errors.New("Not implemented")
)

type PragmaCheckRequest proto.Request

const (
	applyTimeout           = 10 * time.Second
	peersInfoPath          = "raft/peers.info"
	peersPath              = "raft/peers.json"
	connectionPoolCount    = 5
	connectionTimeout      = 10 * time.Second
	trailingScale          = 1.25
	raftDBPath             = "raft.db"
	raftLogCacheSize       = 128
	observerChanLen        = 50
	commitEquivalenceDelay = 50 * time.Millisecond
	snapshotsDirName       = "wsnapshots"
)

const (
	numSnapshots                      = "num_snapshots"
	numSnapshotsFailed                = "num_snapshots_failed"
	numUserSnapshots                  = "num_user_snapshots"
	numUserSnapshotsFailed            = "num_user_snapshots_failed"
	numWALSnapshots                   = "num_wal_snapshots"
	numWALSnapshotsFailed             = "num_wal_snapshots_failed"
	numSnapshotsFull                  = "num_snapshots_full"
	numSnapshotsIncremental           = "num_snapshots_incremental"
	numFullCheckpointFailed           = "num_full_checkpoint_failed"
	numWALCheckpointTruncateFailed    = "num_wal_checkpoint_truncate_failed"
	numAutoVacuums                    = "num_auto_vacuums"
	numAutoVacuumsFailed              = "num_auto_vacuums_failed"
	autoVacuumDuration                = "auto_vacuum_duration"
	numAutoOptimizes                  = "num_auto_optimizes"
	numAutoOptimizesFailed            = "num_auto_optimizes_failed"
	autoOptimizeDuration              = "auto_optimize_duration"
	numBoots                          = "num_boots"
	numBackups                        = "num_backups"
	numLoads                          = "num_loads"
	numRestores                       = "num_restores"
	numRestoresFailed                 = "num_restores_failed"
	numAutoRestores                   = "num_auto_restores"
	numAutoRestoresSkipped            = "num_auto_restores_skipped"
	numAutoRestoresFailed             = "num_auto_restores_failed"
	numRecoveries                     = "num_recoveries"
	numProviderChecks                 = "num_provider_checks"
	numProviderProvides               = "num_provider_provides"
	numProviderProvidesFail           = "num_provider_provides_fail"
	numUncompressedCommands           = "num_uncompressed_commands"
	numCompressedCommands             = "num_compressed_commands"
	numJoins                          = "num_joins"
	numIgnoredJoins                   = "num_ignored_joins"
	numRemovedBeforeJoins             = "num_removed_before_joins"
	numDBStatsErrors                  = "num_db_stats_errors"
	snapshotCreateDuration            = "snapshot_create_duration"
	snapshotCreateChkTruncateDuration = "snapshot_create_chk_truncate_duration"
	snapshotCreateWALCompactDuration  = "snapshot_create_wal_compact_duration"
	numSnapshotPersists               = "num_snapshot_persists"
	numSnapshotPersistsFailed         = "num_snapshot_persists_failed"
	snapshotPersistDuration           = "snapshot_persist_duration"
	snapshotPrecompactWALSize         = "snapshot_precompact_wal_size"
	snapshotWALSize                   = "snapshot_wal_size"
	leaderChangesObserved             = "leader_changes_observed"
	leaderChangesDropped              = "leader_changes_dropped"
	failedHeartbeatObserved           = "failed_heartbeat_observed"
	nodesReapedOK                     = "nodes_reaped_ok"
	nodesReapedFailed                 = "nodes_reaped_failed"
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("store")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numSnapshots, 0)
	stats.Add(numSnapshotsFailed, 0)
	stats.Add(numUserSnapshots, 0)
	stats.Add(numUserSnapshotsFailed, 0)
	stats.Add(numWALSnapshots, 0)
	stats.Add(numWALSnapshotsFailed, 0)
	stats.Add(numSnapshotsFull, 0)
	stats.Add(numSnapshotsIncremental, 0)
	stats.Add(numFullCheckpointFailed, 0)
	stats.Add(numWALCheckpointTruncateFailed, 0)
	stats.Add(numAutoVacuums, 0)
	stats.Add(numAutoVacuumsFailed, 0)
	stats.Add(autoVacuumDuration, 0)
	stats.Add(numAutoOptimizes, 0)
	stats.Add(numAutoOptimizesFailed, 0)
	stats.Add(autoOptimizeDuration, 0)
	stats.Add(numBoots, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoads, 0)
	stats.Add(numRestores, 0)
	stats.Add(numRestoresFailed, 0)
	stats.Add(numRecoveries, 0)
	stats.Add(numProviderChecks, 0)
	stats.Add(numProviderProvides, 0)
	stats.Add(numProviderProvidesFail, 0)
	stats.Add(numAutoRestores, 0)
	stats.Add(numAutoRestoresSkipped, 0)
	stats.Add(numAutoRestoresFailed, 0)
	stats.Add(numUncompressedCommands, 0)
	stats.Add(numCompressedCommands, 0)
	stats.Add(numJoins, 0)
	stats.Add(numIgnoredJoins, 0)
	stats.Add(numRemovedBeforeJoins, 0)
	stats.Add(numDBStatsErrors, 0)
	stats.Add(snapshotCreateDuration, 0)
	stats.Add(snapshotCreateChkTruncateDuration, 0)
	stats.Add(snapshotCreateWALCompactDuration, 0)
	stats.Add(numSnapshotPersists, 0)
	stats.Add(numSnapshotPersistsFailed, 0)
	stats.Add(snapshotPersistDuration, 0)
	stats.Add(snapshotPrecompactWALSize, 0)
	stats.Add(snapshotWALSize, 0)
	stats.Add(leaderChangesObserved, 0)
	stats.Add(leaderChangesDropped, 0)
	stats.Add(failedHeartbeatObserved, 0)
	stats.Add(nodesReapedOK, 0)
	stats.Add(nodesReapedFailed, 0)
}

// ClusterState defines the possible Raft states the current node can be in
type ClusterState int

// Represents the Raft cluster states
const (
	Leader ClusterState = iota
	Follower
	Candidate
	Shutdown
	Unknown
)

// SnapshotStore is the interface Snapshot stores must implement.
type SnapshotStore interface {
	raft.SnapshotStore

	// FullNeeded returns true if a full snapshot is needed.
	FullNeeded() (bool, error)

	// SetFullNeeded explicitly sets that a full snapshot is needed.
	SetFullNeeded() error

	// Stats returns stats about the Snapshot Store.
	Stats() (map[string]interface{}, error)
}

// Wire Store is a BBolt database, where all changes are made via Raft consensus.
type Store struct {
	open          *rsync.AtomicBool
	raftDir       string
	peersPath     string
	peersInfoPath string

	raft   *raft.Raft // The consensus mechanism.
	ly     Layer
	raftTn *NodeTransport
	raftID string // Node ID.

	ShutdownOnRemove     bool
	SnapshotThreshold    uint64
	SnapshotInterval     time.Duration
	LeaderLeaseTimeout   time.Duration
	HeartbeatTimeout     time.Duration
	ElectionTimeout      time.Duration
	ApplyTimeout         time.Duration
	RaftLogLevel         string
	NoFreeListSync       bool
	AutoVacInterval      time.Duration
	AutoOptimizeInterval time.Duration

	// Raft changes observer
	leaderObserversMu sync.RWMutex
	leaderObservers   []chan<- struct{}
	observerClose     chan struct{}
	observerDone      chan struct{}
	observerChan      chan raft.Observation
	observer          *raft.Observer

	firstLogAppliedT time.Time // Time first log is applied
	openT            time.Time // Timestamp when Store opens.

	reqMarshaller *command.RequestMarshaler // Request marshaler for writing to log.
	raftLog       raft.LogStore             // Persistent log store.
	raftStable    raft.StableStore          // Persistent k-v store.
	boltStore     *rlog.Log                 // Physical store.

	// TODO: Create this
	logger zerolog.Logger

	notifyMu        sync.Mutex
	BootstrapExpect int
	bootstrapped    bool
	notifyingNodes  map[string]*Server // List of nodes in the cluster

	// Node-reaping configuration
	ReapTimeout         time.Duration
	ReapReadOnlyTimeout time.Duration

	// Latest log entry index actually reflected by the FSM. Due to Raft code
	// these values are not updated automatically after a Snapshot-restore.
	fsmIdx        *atomic.Uint64
	fsmTarget     *rsync.ReadyTarget[uint64]
	fsmTerm       *atomic.Uint64
	fsmUpdateTime *rsync.AtomicTime // This is node-local time.

	// appendedAtTime is the Leader's clock time when that Leader appended the log entry.
	// The Leader that actually appended the log entry is not necessarily the current Leader.
	appendedAtTime *rsync.AtomicTime

	dbModifiedTime *rsync.AtomicTime // Last time the database file was modified.

	numTrailingLogs uint64

	restorePath   string
	restoreDoneCh chan struct{}

	// Channels that must be closed for the Store to be considered ready.
	readyChans *rsync.ReadyChannels

	// Snapshot
	snapshotDir   string
	snapshotStore SnapshotStore // Snapshot store.

	mu sync.Mutex
}

type Config struct {
	Dir string    // The working directory for raft.
	Tn  Transport // The underlying Transport for raft.
	ID  string    // Node ID.
	// Logger *log.Logger // The logger to use to log stuff.
}

// func New(ly Layer, ko *koanf.Koanf) *Store {
// allocate a new store in memory and initialize
func New(ly Layer, c *Config) *Store {

	// raftDir := ko.String("raft_dir")
	// raftId := ko.String("node_id")
	zlog.Debug().Msg("Creating a new store!")

	// TODO: create a zerolog logger
	logger := zerolog.Logger{}

	return &Store{
		open:            rsync.NewAtomicBool(),
		ly:              ly,
		raftDir:         c.Dir,
		raftID:          c.ID,
		peersPath:       filepath.Join(c.Dir, peersPath),
		peersInfoPath:   filepath.Join(c.Dir, peersInfoPath),
		restoreDoneCh:   make(chan struct{}),
		leaderObservers: make([]chan<- struct{}, 0),
		reqMarshaller:   command.NewRequestMarshaler(),
		notifyingNodes:  make(map[string]*Server),
		ApplyTimeout:    applyTimeout,
		fsmIdx:          &atomic.Uint64{},
		fsmTarget:       rsync.NewReadyTarget[uint64](),
		fsmTerm:         &atomic.Uint64{},
		fsmUpdateTime:   rsync.NewAtomicTime(),
		appendedAtTime:  rsync.NewAtomicTime(),
		dbModifiedTime:  rsync.NewAtomicTime(),
		logger:          logger,
		readyChans:      rsync.NewReadyChannels(),
		snapshotDir:     filepath.Join(c.Dir, snapshotsDirName),
		// snapshotCAS:     rsync.NewCheckAndSet(),
		// Unsure if I need the following data
		// dbAppliedIdx:    &atomic.Uint64{},
		// appliedTarget:   rsync.NewReadyTarget[uint64](),
		// numNoops:        &atomic.Uint64{},
		// numSnapshots:    &atomic.Uint64{},
	}
}

// open the store
func (s *Store) Open() (retError error) {
	defer func() {
		if retError == nil {
			s.open.Set()
		}
	}()

	var err error

	// s.logger.Debug().Msg("Opening the store")
	fmt.Printf("Opening the store\n")

	// Reset/set the defaults
	s.fsmIdx.Store(0)
	s.fsmTarget.Reset()
	s.fsmTerm.Store(0)
	s.fsmUpdateTime.Store(time.Time{})
	s.appendedAtTime.Store(time.Time{})
	s.openT = time.Now()

	// s.logger.Info().Msgf("Opening store with node ID %s, listening on %s", s.raftID, s.ly.Addr().String())

	s.logger.Info().Msgf("Ensuring data directories exist %s", s.raftDir)
	if err := os.MkdirAll(filepath.Dir(s.raftDir), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.peersPath), 0755); err != nil {
		return err
	}

	// Creating network layer
	nt := raft.NewNetworkTransport(NewTransport((s.ly)), connectionPoolCount, connectionTimeout, nil)
	s.raftTn = NewNodeTransport(nt)

	s.numTrailingLogs = uint64(float64(s.SnapshotThreshold) * trailingScale)

	config := s.raftConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Create store for the Snapshots.
	snapshotStore, err := snapshot.NewStore(filepath.Join(s.snapshotDir))
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %s", err)
	}
	snapshotStore.LogReaping = s.hcLogLevel() < hclog.Warn
	s.snapshotStore = snapshotStore
	snaps, err := s.snapshotStore.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %s", err)
	}
	s.logger.Printf("%d preexisting snapshots present", len(snaps))

	// Create the log store and stable store
	s.boltStore, err = rlog.New(filepath.Join(s.raftDir, raftDBPath), s.NoFreeListSync)
	if err != nil {
		return fmt.Errorf("new log store: %s", err)
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)
	if err != nil {
		return fmt.Errorf("new cached store: %s", err)
	}

	// Request to recover node?
	if pathExists(s.peersPath) {
		s.logger.Printf("attempting node recovery using %s", s.peersPath)
		config, err := raft.ReadConfigJSON(s.peersPath)
		if err != nil {
			return fmt.Errorf("failed to read peers file: %s", err.Error())
		}
		s.logger.Debug().Msgf("The config is: %v", config)
		// if err = RecoverNode(s.raftDir, s.logger, s.raftLog, s.boltStore, s.snapshotStore, s.raftTn, config); err != nil {
		// 	return fmt.Errorf("failed to recover node: %s", err.Error())
		// }
		// if err := os.Rename(s.peersPath, s.peersInfoPath); err != nil {
		// 	return fmt.Errorf("failed to move %s after recovery: %s", s.peersPath, err.Error())
		// }
		// s.logger.Printf("node recovered successfully using %s", s.peersPath)
		stats.Add(numRecoveries, 1)
	}

	if s.raftLog == nil || s.raftStable == nil || s.raftTn == nil {
		fmt.Print("Something is wrong")
		return fmt.Errorf("error something went horribly wrong")
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, NewFSM(s), s.raftLog, s.raftStable, s.snapshotStore, s.raftTn)
	if err != nil {
		return fmt.Errorf("creating the raft system failed: %s", err)
	}
	s.raft = ra

	// Open the observer channels.
	s.observerChan = make(chan raft.Observation, observerChanLen)
	s.observer = raft.NewObserver(s.observerChan, false, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		return isLeaderChange || isFailedHeartBeat
	})

	// Register and listen for leader changes.
	s.raft.RegisterObserver(s.observer)
	s.observerClose, s.observerDone = s.observe()

	return nil
}

// raftConfig returns a new Raft config for the store.
func (s *Store) raftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.ShutdownOnRemove = s.ShutdownOnRemove
	config.LogLevel = s.RaftLogLevel
	if s.SnapshotThreshold != 0 {
		config.SnapshotThreshold = s.SnapshotThreshold
		config.TrailingLogs = s.numTrailingLogs
	}
	if s.SnapshotInterval != 0 {
		config.SnapshotInterval = s.SnapshotInterval
	}
	if s.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = s.LeaderLeaseTimeout
	}
	if s.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = s.HeartbeatTimeout
	}
	if s.ElectionTimeout != 0 {
		config.ElectionTimeout = s.ElectionTimeout
	}
	opts := hclog.DefaultOptions
	opts.Name = ""
	opts.Level = s.hcLogLevel()
	config.Logger = hclog.FromStandardLogger(log.New(os.Stderr, "[raft] ", log.LstdFlags), opts)
	return config
}

func (s *Store) hcLogLevel() hclog.Level {
	return hclog.LevelFromString(s.RaftLogLevel)
}

// pathExists returns true if the given path exists.
func pathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func (s *Store) observe() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case o := <-s.observerChan:
				switch signal := o.Data.(type) {
				case raft.FailedHeartbeatObservation:
					stats.Add(failedHeartbeatObserved, 1)

					nodes, err := s.Nodes()
					if err != nil {
						s.logger.Info().Msgf("failed to get nodes configuration during reap check: %s", err.Error())
					}
					servers := Servers(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)

					isReadOnly, found := servers.IsReadOnly(id)
					if !found {
						s.logger.Info().Msgf("node %s (failing heartbeat) is not present in configuration", id)
						break
					}

					if (isReadOnly && s.ReapReadOnlyTimeout > 0 && dur > s.ReapReadOnlyTimeout) ||
						(!isReadOnly && s.ReapTimeout > 0 && dur > s.ReapTimeout) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := s.remove(id); err != nil {
							stats.Add(nodesReapedFailed, 1)
							s.logger.Printf("failed to reap %s %s: %s", pn, id, err.Error())
						} else {
							stats.Add(nodesReapedOK, 1)
							s.logger.Printf("successfully reaped %s %s", pn, id)
						}
					}
				case raft.LeaderObservation:
					s.leaderObserversMu.RLock()
					for i := range s.leaderObservers {
						select {
						case s.leaderObservers[i] <- struct{}{}:
							stats.Add(leaderChangesObserved, 1)
						default:
							stats.Add(leaderChangesDropped, 1)
						}
					}
					s.leaderObserversMu.RUnlock()
					s.selfLeaderChange(signal.LeaderID == raft.ServerID(s.raftID))
					if signal.LeaderID == raft.ServerID(s.raftID) {
						s.logger.Printf("this node (ID=%s) is now Leader", s.raftID)
					} else {
						if signal.LeaderID == "" {
							s.logger.Printf("Leader is now unknown")
						} else {
							s.logger.Printf("node %s is now Leader", signal.LeaderID)
						}
					}
				}

			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

// Nodes returns the slice of nodes in the cluster, sorted by ID ascending.
func (s *Store) Nodes() ([]*Server, error) {
	if !s.open.Is() {
		return nil, ErrNotOpen
	}

	s.logger.Debug().Msg("a node exists!")

	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil, f.Error()
	}

	rs := f.Configuration().Servers
	servers := make([]*Server, len(rs))
	for i := range rs {
		servers[i] = &Server{
			ID:       string(rs[i].ID),
			Addr:     string(rs[i].Address),
			Suffrage: rs[i].Suffrage.String(),
		}
	}

	sort.Sort(Servers(servers))
	return servers, nil
}

// selfLeaderChange is called when this node detects that its leadership
// status has changed.
func (s *Store) selfLeaderChange(leader bool) {
	if s.restorePath != "" {
		defer func() {
			// Whatever happens, this is a one-shot attempt to perform a restore
			err := os.Remove(s.restorePath)
			if err != nil {
				s.logger.Printf("failed to remove restore path after restore %s: %s",
					s.restorePath, err.Error())
			}
			s.restorePath = ""
			close(s.restoreDoneCh)
		}()

		if !leader {
			s.logger.Printf("different node became leader, not performing auto-restore")
			stats.Add(numAutoRestoresSkipped, 1)
		} else {
			s.logger.Printf("this node is now leader, auto-restoring from %s", s.restorePath)
			if err := s.installRestore(); err != nil {
				s.logger.Printf("failed to auto-restore from %s: %s", s.restorePath, err.Error())
				stats.Add(numAutoRestoresFailed, 1)
				return
			}
			stats.Add(numAutoRestores, 1)
			s.logger.Printf("node auto-restored successfully from %s", s.restorePath)
		}
	}
}

func (s *Store) installRestore() error {
	f, err := os.Open(s.restorePath)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	lr := &proto.LoadRequest{
		Data: b,
	}
	return s.load(lr)
}

func (s *Store) load(lr *proto.LoadRequest) error {
	return nil
}

func (s *Store) remove(id string) error {
	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil && f.Error() == raft.ErrNotLeader {
		return ErrNotLeader
	}
	return f.Error()
}

func IsNewNode(raftDir string) bool {
	// If there is any preexisting Raft state, then this node
	// has already been created.
	return !pathExists(filepath.Join(raftDir, raftDBPath))
}

// Implementation of the manager

// LeaderAddr returns the address of the current leader. Returns a
// blank string if there is no leader or if the Store is not open.
func (s *Store) LeaderAddr() (string, error) {
	if s.open.Is() {
		return "", nil
	}
	addr, _ := s.raft.LeaderWithID()
	return string(addr), nil
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	if !s.open.Is() {
		return "", nil
	}
	_, id := s.raft.LeaderWithID()
	return string(id), nil
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Store) LeaderWithID() (string, string) {
	if !s.open.Is() {
		return "", ""
	}
	addr, id := s.raft.LeaderWithID()
	return string(addr), string(id)
}

// HasLeaderID returns true if the cluster has a leader ID, false otherwise.
func (s *Store) HasLeaderID() bool {
	id, err := s.LeaderID()
	if err != nil {
		s.logger.Err(err).Msg("Error when getting leader id")
		return false
	}
	return id != ""
}

// LeaderCommitIndex returns the Raft leader commit index, as indicated
// by the latest AppendEntries RPC. If this node is the Leader then the
// commit index is returned directly from the Raft object.
func (s *Store) LeaderCommitIndex() (uint64, error) {
	if !s.open.Is() {
		return 0, ErrNotOpen
	}
	if s.raft.State() == raft.Leader {
		return s.raft.CommitIndex(), nil
	}
	return s.raftTn.LeaderCommitIndex(), nil
}

func (s *Store) CommitIndex() (uint64, error) {
	if !s.open.Is() {
		return 0, ErrNotOpen
	}
	return s.raft.CommitIndex(), nil
}

func (s *Store) Remove(rn *commandProto.RemoveNodeRequest) error {
	return ErrNotImplemented
}

func (s *Store) Notify(n *commandProto.NotifyRequest) error {
	return ErrNotImplemented
}

func (s *Store) Join(n *commandProto.JoinRequest) error {
	return nil
}

// Implementation for the HTTP daemon

// Snapshot performs a snapshot, leaving n trailing logs behind. If n
// is greater than zero, that many logs are left in the log after
// snapshotting. If n is zero, then the number set at Store creation is used.
// Finally, once this function returns, the trailing log configuration value
// is reset to the value set at Store creation.
func (s *Store) Snapshot(n uint64) (retError error) {
	defer func() {
		if retError != nil {
			stats.Add(numUserSnapshotsFailed, 1)
			s.logger.Printf("failed to generate user-requested snapshot: %s", retError.Error())
		}
	}()

	if n > 0 {
		cfg := s.raft.ReloadableConfig()
		defer func() {
			cfg.TrailingLogs = s.numTrailingLogs
			if err := s.raft.ReloadConfig(cfg); err != nil {
				s.logger.Printf("failed to reload Raft config: %s", err.Error())
			}
		}()
		cfg.TrailingLogs = n
		if err := s.raft.ReloadConfig(cfg); err != nil {
			return fmt.Errorf("failed to reload Raft config: %s", err.Error())
		}
	}
	if err := s.raft.Snapshot().Error(); err != nil {
		if strings.Contains(err.Error(), ErrLoadInProgress.Error()) {
			return ErrLoadInProgress
		}
		return err
	}
	stats.Add(numUserSnapshots, 1)
	return nil
}

func (s *Store) Backup(br *proto.BackupRequest, dst io.Writer) (retErr error) {
	return ErrNotImplemented
}

func (s *Store) Ready() bool {
	// if and and has a leader
	return s.open.Is() && s.readyChans.Ready() && s.HasLeader()
}

// HasLeader returns true if the cluster has a leader, false otherwise.
func (s *Store) HasLeader() bool {
	if !s.open.Is() {
		return false
	}
	return s.raft.Leader() != ""
}

// Committed blocks until the local commit index is greater than or
// equal to the Leader index, as checked when the function is called.
// It returns the committed index. If the Leader index is 0, then the
// system waits until the commit index is at least 1.
func (s *Store) Committed(timeout time.Duration) (uint64, error) {
	lci, err := s.LeaderCommitIndex()
	if err != nil {
		return lci, err
	}
	return lci, s.WaitForCommitIndex(max(1, lci), timeout)
}

// WaitForCommitIndex blocks until the local Raft commit index is equal to
// or greater the given index, or the timeout expires.
func (s *Store) WaitForCommitIndex(idx uint64, timeout time.Duration) error {
	check := func() bool {
		return s.raft.CommitIndex() >= idx
	}
	return rsync.NewPollTrue(check, commitEquivalenceDelay, timeout).Run("commit index")
}

// Addr returns the address of the store.
func (s *Store) Addr() string {
	if !s.open.Is() {
		return ""
	}
	return string(s.raftTn.LocalAddr())
}

// logSize returns the size of the Raft log on disk.
func (s *Store) logSize() (int64, error) {
	fi, err := os.Stat(filepath.Join(s.raftDir, raftDBPath))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// IsVoter returns true if the current node is a voter in the cluster. If there
// is no reference to the current node in the current cluster configuration then
// false will also be returned.
func (s *Store) IsVoter() (bool, error) {
	if !s.open.Is() {
		return false, ErrNotOpen
	}

	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false, err
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == raft.ServerID(s.raftID) {
			return srv.Suffrage == raft.Voter, nil
		}
	}
	return false, nil
}

// Stats returns stats for the store.
func (s *Store) Stats() (map[string]interface{}, error) {
	if !s.open.Is() {
		return map[string]interface{}{
			"open": false,
		}, nil
	}

	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}
	leaderAddr, leaderID := s.LeaderWithID()

	// Perform type-conversion to actual numbers where possible.
	raftStats := make(map[string]interface{})
	for k, v := range s.raft.Stats() {
		if s, err := strconv.ParseInt(v, 10, 64); err != nil {
			raftStats[k] = v
		} else {
			raftStats[k] = s
		}
	}
	raftStats["log_size"], err = s.logSize()
	if err != nil {
		return nil, err
	}
	raftStats["voter"], err = s.IsVoter()
	if err != nil {
		return nil, err
	}
	raftStats["bolt"] = s.boltStore.Stats()
	raftStats["transport"] = s.raftTn.Stats()

	dirSz, err := utils.DirSize(s.raftDir)
	if err != nil {
		return nil, err
	}

	status := map[string]interface{}{
		"open":            s.open,
		"node_id":         s.raftID,
		"raft":            raftStats,
		"fsm_index":       s.fsmIdx.Load(),
		"fsm_term":        s.fsmTerm.Load(),
		"fsm_update_time": s.fsmUpdateTime.Load(),
		"addr":            s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    leaderAddr,
		},
		"leader_appended_at_time": s.appendedAtTime.Load(),
		"ready":                   s.Ready(),
		"observer": map[string]uint64{
			"observed": s.observer.GetNumObserved(),
			"dropped":  s.observer.GetNumDropped(),
		},
		"apply_timeout":      s.ApplyTimeout.String(),
		"heartbeat_timeout":  s.HeartbeatTimeout.String(),
		"election_timeout":   s.ElectionTimeout.String(),
		"snapshot_threshold": s.SnapshotThreshold,
		"snapshot_interval":  s.SnapshotInterval.String(),
		// "snapshot_cas":           s.snapshotCAS.Stats(),
		"reap_timeout":           s.ReapTimeout.String(),
		"reap_read_only_timeout": s.ReapReadOnlyTimeout.String(),
		"no_freelist_sync":       s.NoFreeListSync,
		"trailing_logs":          s.numTrailingLogs,
		"request_marshaler":      s.reqMarshaller.Stats(),
		"nodes":                  nodes,
		"dir":                    s.raftDir,
		"dir_size":               dirSz,
		"dir_size_friendly":      utils.FriendlyBytes(uint64(dirSz)),
	}

	return status, nil
}

// Cluster interface implementation

// Execute executes queries that return no rows, but do modify the database.
// func (s *Store) Execute(ex *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
// 	p := (*PragmaCheckRequest)(ex.Request)
// 	if err := p.Check(); err != nil {
// 		return nil, err
// 	}

// 	if !s.open.Is() {
// 		return nil, ErrNotOpen
// 	}

// 	if s.raft.State() != raft.Leader {
// 		return nil, ErrNotLeader
// 	}
// 	if !s.Ready() {
// 		return nil, ErrNotReady
// 	}
// 	return s.execute(ex)
// }

// type fsmExecuteQueryResponse struct {
// 	results []*proto.ExecuteQueryResponse
// 	error   error
// }

// func (s *Store) execute(ex *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
// 	b, compressed, err := s.tryCompress(ex)
// 	if err != nil {
// 		return nil, err
// 	}

// 	c := &proto.Command{
// 		Type:       proto.Command_COMMAND_TYPE_EXECUTE,
// 		SubCommand: b,
// 		Compressed: compressed,
// 	}

// 	b, err = command.Marshal(c)
// 	if err != nil {
// 		return nil, err
// 	}

// 	af := s.raft.Apply(b, s.ApplyTimeout)
// 	if af.Error() != nil {
// 		if af.Error() == raft.ErrNotLeader {
// 			return nil, ErrNotLeader
// 		}
// 		return nil, af.Error()
// 	}
// 	r := af.Response().(*fsmExecuteQueryResponse)
// 	return r.results, r.error
// }

// tryCompress attempts to compress the given command. If the command is
// successfully compressed, the compressed byte slice is returned, along with
// a boolean true. If the command cannot be compressed, the uncompressed byte
// slice is returned, along with a boolean false. The stats are updated
// accordingly.
func (s *Store) tryCompress(rq command.Requester) ([]byte, bool, error) {
	b, compressed, err := s.reqMarshaller.Marshal(rq)
	if err != nil {
		return nil, false, err
	}
	if compressed {
		stats.Add(numCompressedCommands, 1)
	} else {
		stats.Add(numUncompressedCommands, 1)
	}
	return b, compressed, nil
}

// ID returns the Raft ID of the store.
func (s *Store) ID() string {
	return s.raftID
}

// Bootstrap executes a cluster bootstrap on this node, using the given
// Servers as the configuration.
func (s *Store) Bootstrap(servers ...*Server) error {
	raftServers := make([]raft.Server, len(servers))
	for i := range servers {
		raftServers[i] = raft.Server{
			ID:      raft.ServerID(servers[i].ID),
			Address: raft.ServerAddress(servers[i].Addr),
		}
	}
	fut := s.raft.BootstrapCluster(raft.Configuration{
		Servers: raftServers,
	})
	return fut.Error()
}
