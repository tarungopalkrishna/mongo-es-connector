// print-cpuburn-graph emits a base64-encoded msgpack rpc.JobGraph for a
// Source -> Map(cpu-burn) -> Sink pipeline configured to do measurable
// CPU work per event. Paired with the cpu-burn map operator registered
// by wire-worker-example, this produces jobs that take seconds rather
// than microseconds — useful for load-testing the scheduler/dispatch
// path with realistic latency tails.
//
// Usage:
//
//	print-cpuburn-graph                                  # defaults
//	print-cpuburn-graph --rounds 100000 --events 200     # heavier
//	print-cpuburn-graph --rounds 5000   --events 10      # lighter
//
// The output is the value the coordinator's POST /api/v1/jobs endpoint
// expects in the "graph_bytes" field.
package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"math"
	"os"

	"github.com/tarungka/wire/internal/protocol"
	"github.com/tarungka/wire/internal/rpc"
	"github.com/tarungka/wire/sdk/connectors/memory"
)

// CPUBurnConfig mirrors examples/wire-worker-example.CPUBurnConfig.
// Duplicated to keep this binary independent of the worker example.
type CPUBurnConfig struct {
	Rounds uint32 `codec:"r"`
}

func main() {
	rounds := flag.Uint("rounds", 50000, "SHA-256 iterations per event (CPU work knob)")
	events := flag.Int("events", 100, "events the source emits per job (latency knob)")
	payload := flag.Int("payload-bytes", 64, "size of each event's payload in bytes")
	flag.Parse()

	if *rounds == 0 || uint64(*rounds) > math.MaxUint32 {
		fmt.Fprintln(os.Stderr, "rounds must be between 1 and 4294967295")
		os.Exit(2)
	}
	if *events <= 0 {
		fmt.Fprintln(os.Stderr, "events must be > 0")
		os.Exit(2)
	}
	if *payload <= 0 {
		fmt.Fprintln(os.Stderr, "payload-bytes must be > 0")
		os.Exit(2)
	}

	srcEvents := make([][]byte, *events)
	for i := range srcEvents {
		buf := make([]byte, *payload)
		// Fill with a position-dependent byte so each event is distinct
		// (prevents the hash chain from collapsing to a constant).
		for j := range buf {
			buf[j] = byte((i + j) & 0xff)
		}
		srcEvents[i] = buf
	}

	srcCfg, err := protocol.EncodeMsgPack(memory.SourceConfig{Events: srcEvents})
	check(err)
	burnCfg, err := protocol.EncodeMsgPack(CPUBurnConfig{Rounds: uint32(*rounds)}) // #nosec G115 -- validated within uint32 range
	check(err)
	sinkCfg, err := protocol.EncodeMsgPack(memory.SinkConfig{SinkID: "loadtest-cpu"})
	check(err)

	graph := rpc.JobGraph{
		Operators: []rpc.OperatorDescriptor{
			{OperatorID: "src", Name: "src", Type: rpc.OperatorTypeSource, Parallelism: 1, ClassName: "memory-source", Config: srcCfg},
			{OperatorID: "burn", Name: "burn", Type: rpc.OperatorTypeMap, Parallelism: 1, ClassName: "cpu-burn", Config: burnCfg},
			{OperatorID: "sink", Name: "sink", Type: rpc.OperatorTypeSink, Parallelism: 1, ClassName: "memory-sink", Config: sinkCfg},
		},
		Edges: []rpc.EdgeDescriptor{
			{SourceOperatorID: "src", TargetOperatorID: "burn", Shuffle: rpc.ShuffleStrategyForward},
			{SourceOperatorID: "burn", TargetOperatorID: "sink", Shuffle: rpc.ShuffleStrategyForward},
		},
	}
	data, err := protocol.EncodeMsgPack(&graph)
	check(err)
	fmt.Println(base64.StdEncoding.EncodeToString(data))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
