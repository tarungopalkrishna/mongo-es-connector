package cluster

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/tarungka/wire/internal/cluster/proto"
	commandProto "github.com/tarungka/wire/internal/command/proto"
	pb "google.golang.org/protobuf/proto"
)

// stats captures stats for the Cluster service.
var stats *expvar.Map

const (
	numGetNodeAPIRequest  = "num_get_node_api_req"
	numGetNodeAPIResponse = "num_get_node_api_resp"
	numExecuteRequest     = "num_execute_req"
	numQueryRequest       = "num_query_req"
	numRequestRequest     = "num_request_req"
	numBackupRequest      = "num_backup_req"
	numLoadRequest        = "num_load_req"
	numRemoveNodeRequest  = "num_remove_node_req"
	numNotifyRequest      = "num_notify_req"
	numJoinRequest        = "num_join_req"

	numClientRetries            = "num_client_retries"
	numGetNodeAPIRequestRetries = "num_get_node_api_req_retries"
	numClientLoadRetries        = "num_client_load_retries"
	numClientExecuteRetries     = "num_client_execute_retries"
	numClientQueryRetries       = "num_client_query_retries"
	numClientRequestRetries     = "num_client_request_retries"
	numClientReadTimeouts       = "num_client_read_timeouts"
	numClientWriteTimeouts      = "num_client_write_timeouts"

	// Client stats for this package.
	numGetNodeAPIRequestLocal = "num_get_node_api_req_local"
)

const (
	// MuxRaftHeader is the byte used to indicate internode Raft communications.
	MuxRaftHeader = 1

	// MuxClusterHeader is the byte used to request internode cluster state information.
	MuxClusterHeader = 2 // Cluster state communications
)

func init() {
	stats = expvar.NewMap("cluster")
	stats.Add(numGetNodeAPIRequest, 0)
	stats.Add(numGetNodeAPIResponse, 0)
	stats.Add(numExecuteRequest, 0)
	stats.Add(numQueryRequest, 0)
	stats.Add(numRequestRequest, 0)
	stats.Add(numBackupRequest, 0)
	stats.Add(numLoadRequest, 0)
	stats.Add(numRemoveNodeRequest, 0)
	stats.Add(numGetNodeAPIRequestLocal, 0)
	stats.Add(numNotifyRequest, 0)
	stats.Add(numJoinRequest, 0)
	stats.Add(numClientRetries, 0)
	stats.Add(numGetNodeAPIRequestRetries, 0)
	stats.Add(numClientLoadRetries, 0)
	stats.Add(numClientExecuteRetries, 0)
	stats.Add(numClientQueryRetries, 0)
	stats.Add(numClientRequestRetries, 0)
	stats.Add(numClientReadTimeouts, 0)
	stats.Add(numClientWriteTimeouts, 0)
}

// Dialer is the interface dialers must implement.
type Dialer interface {
	// Dial is used to create a connection to a service listening
	// on an address.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Database is the interface any queryable system must implement
type Database interface {
	// Execute executes a slice of SQL statements.
	Execute(er *commandProto.ExecuteRequest) ([]*commandProto.ExecuteQueryResponse, error)

	// Query executes a slice of queries, each of which returns rows.
	Query(qr *commandProto.QueryRequest) ([]*commandProto.QueryRows, error)

	// Request processes a request that can both executes and queries.
	Request(rr *commandProto.ExecuteQueryRequest) ([]*commandProto.ExecuteQueryResponse, error)

	// Backup writes a backup of the database to the writer.
	Backup(br *commandProto.BackupRequest, dst io.Writer) error

	// Loads an entire SQLite file into the database
	Load(lr *commandProto.LoadRequest) error
}

// Manager is the interface node-management systems must implement
type Manager interface {
	// LeaderAddr returns the Raft address of the leader of the cluster.
	LeaderAddr() (string, error)

	// CommitIndex returns the Raft commit index of the cluster.
	CommitIndex() (uint64, error)

	// Remove removes the node, given by id, from the cluster
	Remove(rn *commandProto.RemoveNodeRequest) error

	// Notify notifies this node that a remote node is ready
	// for bootstrapping.
	Notify(n *commandProto.NotifyRequest) error

	// Join joins a remote node to the cluster.
	Join(n *commandProto.JoinRequest) error
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// AA authenticates and checks authorization for the given perm.
	AA(username, password, perm string) bool
}

// Service provides information about the node and cluster.
type Service struct {
	ln   net.Listener // Incoming connections to the service
	addr net.Addr     // Address on which this service is listening

	mgr Manager  // The cluster management system.

	mu      sync.RWMutex
	https   bool   // Serving HTTPS?
	apiAddr string // host:port this node serves the HTTP API.

	logger *log.Logger
	// logger zerolog.Logger
}

// New returns a new instance of the cluster service
func New(ln net.Listener,  m Manager) *Service {
	return &Service{
		ln:              ln,
		addr:            ln.Addr(),
		mgr:             m,
		logger:          log.New(os.Stderr, "[cluster] ", log.LstdFlags),
		// logger:          zerolog.Logger{},
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	go s.serve()
	s.logger.Println("service listening on", s.addr)
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
}

// EnableHTTPS tells the cluster service the API serves HTTPS.
func (s *Service) EnableHTTPS(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.https = b
}

// SetAPIAddr sets the API address the cluster service returns.
func (s *Service) SetAPIAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Printf("Setting the api address as %s", addr)
	s.apiAddr = addr
}

// GetAPIAddr returns the previously-set API address
func (s *Service) GetAPIAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.apiAddr
}

// GetNodeAPIURL returns fully-specified HTTP(S) API URL for the
// node running this service.
func (s *Service) GetNodeAPIURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scheme := "http"
	if s.https {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, s.apiAddr)
}

// Stats returns status of the Service.
func (s *Service) Stats() (map[string]interface{}, error) {
	st := map[string]interface{}{
		"addr":     s.addr.String(),
		"https":    strconv.FormatBool(s.https),
		"api_addr": s.apiAddr,
	}

	return st, nil
}

func (s *Service) serve() error {
	for {
		s.logger.Println("waiting for request")
		conn, err := s.ln.Accept() // I think this is blocking until a request
		if err != nil {
			return err
		}
		s.logger.Println("up and accepting requests")

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()

	b := make([]byte, protoBufferLengthSize)
	for {
		_, err := io.ReadFull(conn, b)
		if err != nil {
			return
		}
		sz := binary.LittleEndian.Uint64(b[0:])

		p := make([]byte, sz)
		_, err = io.ReadFull(conn, p)
		if err != nil {
			return
		}

		c := &proto.Command{}
		err = pb.Unmarshal(p, c)
		if err != nil {
			conn.Close()
		}

		switch c.Type {
		case proto.Command_COMMAND_TYPE_GET_NODE_API_URL:
			stats.Add(numGetNodeAPIRequest, 1)
			ci, err := s.mgr.CommitIndex()
			if err != nil {
				conn.Close()
				return
			}
			p, err = pb.Marshal(&proto.NodeMeta{
				Url:         s.GetNodeAPIURL(),
				CommitIndex: ci,
			})
			if err != nil {
				conn.Close()
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
			stats.Add(numGetNodeAPIResponse, 1)

		case proto.Command_COMMAND_TYPE_LOAD_CHUNK:
			resp := &proto.CommandLoadChunkResponse{
				Error: "unsupported",
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_REMOVE_NODE:
			stats.Add(numRemoveNodeRequest, 1)
			resp := &proto.CommandRemoveNodeResponse{}

			rn := c.GetRemoveNodeRequest()
			if rn == nil {
				resp.Error = "LoadRequest is nil"
			} else if !true {
				resp.Error = "unauthorized"
			} else {
				if err := s.mgr.Remove(rn); err != nil {
					resp.Error = err.Error()
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_NOTIFY:
			stats.Add(numNotifyRequest, 1)
			resp := &proto.CommandNotifyResponse{}

			nr := c.GetNotifyRequest()
			if nr == nil {
				resp.Error = "NotifyRequest is nil"
			} else if !true {
				resp.Error = "unauthorized"
			} else {
				if err := s.mgr.Notify(nr); err != nil {
					resp.Error = err.Error()
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_JOIN:
			stats.Add(numJoinRequest, 1)
			resp := &proto.CommandJoinResponse{}

			jr := c.GetJoinRequest()
			if jr == nil {
				resp.Error = "JoinRequest is nil"
			} else {
				// TODO: These logics are wrong, need to rewrite them
				if (jr.Voter && true) ||
					(!jr.Voter && true) {
					if err := s.mgr.Join(jr); err != nil {
						resp.Error = err.Error()
						if err.Error() == "not leader" {
							laddr, err := s.mgr.LeaderAddr()
							if err != nil {
								resp.Error = err.Error()
							} else {
								resp.Leader = laddr
							}
						}
					}
				} else {
					resp.Error = "unauthorized"
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}
		}
	}
}

func marshalAndWrite(conn net.Conn, m pb.Message) error {
	p, err := pb.Marshal(m)
	if err != nil {
		conn.Close()
		return err
	}
	return writeBytesWithLength(conn, p)
}

func writeBytesWithLength(conn net.Conn, p []byte) error {
	b := make([]byte, protoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err := conn.Write(b)
	if err != nil {
		return err
	}
	_, err = conn.Write(p)
	return err
}

// gzCompress compresses the given byte slice.
func gzCompress(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip new writer: %s", err)
	}

	if _, err := gzw.Write(b); err != nil {
		return nil, fmt.Errorf("gzip Write: %s", err)
	}
	if err := gzw.Close(); err != nil {
		return nil, fmt.Errorf("gzip Close: %s", err)
	}
	return buf.Bytes(), nil
}
