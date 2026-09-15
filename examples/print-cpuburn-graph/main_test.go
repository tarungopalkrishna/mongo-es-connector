package main

import (
	"encoding/base64"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/tarungka/wire/internal/protocol"
	"github.com/tarungka/wire/internal/rpc"
)

func TestGraphRounds(t *testing.T) {
	if os.Getenv("WIRE_TEST_GRAPH_HELPER") == "1" {
		flag.CommandLine = flag.NewFlagSet("graph", flag.ExitOnError)
		os.Args = []string{"graph", "--rounds", os.Getenv("WIRE_TEST_ROUNDS"), "--events", "1", "--payload-bytes", "1"}
		main()
		os.Exit(0)
	}
	for _, tc := range []struct {
		rounds string
		valid  bool
		want   uint32
	}{
		{"0", false, 0}, {"4294967296", false, 0}, {"1", true, 1}, {"4294967295", true, 4294967295},
	} {
		t.Run(tc.rounds, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestGraphRounds$")
			cmd.Env = append(os.Environ(), "WIRE_TEST_GRAPH_HELPER=1", "WIRE_TEST_ROUNDS="+tc.rounds)
			output, err := cmd.CombinedOutput()
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid rounds accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
			if err != nil {
				t.Fatal(err)
			}
			var graph rpc.JobGraph
			if err := protocol.DecodeMsgPack(raw, &graph); err != nil {
				t.Fatal(err)
			}
			var cfg CPUBurnConfig
			if err := protocol.DecodeMsgPack(graph.Operators[1].Config, &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.Rounds != tc.want {
				t.Fatalf("rounds=%d want=%d", cfg.Rounds, tc.want)
			}
		})
	}
}
