// wire-worker-example is a runnable demo of the operator-registry pattern.
//
// It registers the in-tree memory-source and memory-sink connectors plus a
// trivial "upper" map operator, then runs as a Wire worker that connects to
// a coordinator. Pair it with the submit-uppercase-job example (or any
// SDK program in Cluster mode) to see end-to-end execution.
//
// Usage:
//
//	go run ./examples/wire-worker-example \
//	    --coordinator-addr 127.0.0.1:4002 \
//	    --task-slots 4
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/tarungka/wire/internal/engine"
	"github.com/tarungka/wire/internal/logger"
	"github.com/tarungka/wire/internal/protocol"
	"github.com/tarungka/wire/internal/worker"
	"github.com/tarungka/wire/sdk/connectors/memory"
)

func main() {
	var (
		coordinatorAddr = flag.String("coordinator-addr", "127.0.0.1:4002", "coordinator wire-protocol address")
		workerID        = flag.String("worker-id", "", "worker ID (defaults to hostname)")
		listenAddr      = flag.String("listen", "127.0.0.1:0", "data-plane listen address (Phase 2+)")
		taskSlots       = flag.Int("task-slots", 4, "number of concurrent task slots")
	)
	flag.Parse()

	// Use the same logger configuration as the wire CLI so both processes
	// produce identical output. We don't open a file — logger.GetLogger
	// handles a nil log file gracefully (writes only to the console).
	log := logger.GetLogger("worker-example")

	// Register the operators this worker can run. In a real deployment,
	// users add their own factories alongside (or instead of) these.
	worker.RegisterSource("memory-source", memory.SourceFactory())
	worker.RegisterSink("memory-sink", memory.SinkFactory())
	worker.RegisterMap("upper", upperMapFactory())
	worker.RegisterMap("cpu-burn", cpuBurnMapFactory())

	w := worker.New(worker.Config{
		WorkerID:        *workerID,
		CoordinatorAddr: *coordinatorAddr,
		ListenAddr:      *listenAddr,
		TaskSlots:       *taskSlots,
	}, log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info().Str("coordinator", *coordinatorAddr).Int("slots", *taskSlots).Msg("starting wire-worker-example")
	if err := w.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "worker exited: %v\n", err)
		os.Exit(1)
	}
}

// upperMapFactory returns a MapFactory whose operator uppercases each event's value.
func upperMapFactory() worker.MapFactory {
	return func(_ context.Context, _ []byte, _ worker.TaskContext) (engine.MapOperator, error) {
		return &upperMap{}, nil
	}
}

type upperMap struct{}

func (*upperMap) Open(_ context.Context) error        { return nil }
func (*upperMap) Close() error                        { return nil }
func (*upperMap) Checkpoint(_ uint64) ([]byte, error) { return nil, nil }
func (*upperMap) Map(_ context.Context, e engine.Event) (engine.Event, error) {
	e.Value = []byte(strings.ToUpper(string(e.Value)))
	return e, nil
}

var _ engine.MapOperator = (*upperMap)(nil)

// CPUBurnConfig is the msgpack payload understood by the cpu-burn map
// operator. Rounds == 0 falls back to a sensible default so a graph
// without explicit config still produces measurable work.
type CPUBurnConfig struct {
	Rounds uint32 `codec:"r"`
}

const defaultCPUBurnRounds = 50_000

// cpuBurnMapFactory returns a MapFactory whose operator hashes each
// event's value Rounds times in a tight SHA-256 loop. The loop is
// data-dependent (each round hashes the previous hash) so the compiler
// can't elide it. Used to build a CPU-intensive workload for load
// testing — paired with print-cpuburn-graph.
func cpuBurnMapFactory() worker.MapFactory {
	return func(_ context.Context, cfgBytes []byte, _ worker.TaskContext) (engine.MapOperator, error) {
		rounds := uint32(defaultCPUBurnRounds)
		if len(cfgBytes) > 0 {
			var cfg CPUBurnConfig
			if err := protocol.DecodeMsgPack(cfgBytes, &cfg); err != nil {
				return nil, fmt.Errorf("cpu-burn: decode config: %w", err)
			}
			if cfg.Rounds > 0 {
				rounds = cfg.Rounds
			}
		}
		return &cpuBurnMap{rounds: rounds}, nil
	}
}

type cpuBurnMap struct {
	rounds uint32
}

func (*cpuBurnMap) Open(_ context.Context) error        { return nil }
func (*cpuBurnMap) Close() error                        { return nil }
func (*cpuBurnMap) Checkpoint(_ uint64) ([]byte, error) { return nil, nil }
func (m *cpuBurnMap) Map(ctx context.Context, e engine.Event) (engine.Event, error) {
	if err := ctx.Err(); err != nil {
		return engine.Event{}, err
	}
	h := sha256.Sum256(e.Value)
	for i := uint32(0); i < m.rounds; i++ {
		// Bound cancellation latency without checking on every hash.
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return engine.Event{}, err
			}
		}
		h = sha256.Sum256(h[:])
	}
	e.Value = h[:]
	return e, nil
}

var _ engine.MapOperator = (*cpuBurnMap)(nil)
