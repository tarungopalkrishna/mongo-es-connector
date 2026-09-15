package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/tarungka/wire/internal/engine"
)

// Cancel on a later poll to exercise cancellation inside the hash loop
// deterministically, without a sleep or a machine-dependent workload duration.
type cancelAfterPolls struct {
	context.Context
	polls int
}

func (c *cancelAfterPolls) Err() error {
	c.polls++
	if c.polls >= 3 {
		return context.Canceled
	}
	return nil
}

func TestCPUBurnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{ctx, &cancelAfterPolls{Context: context.Background()}} {
		_, err := (&cpuBurnMap{rounds: 10000}).Map(ctx, engine.Event{Value: []byte("record")})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	}
}

func TestCPUBurnPreservesHashResult(t *testing.T) {
	input := []byte("record")
	expected := sha256.Sum256(input)
	for i := 0; i < 2049; i++ {
		expected = sha256.Sum256(expected[:])
	}
	got, err := (&cpuBurnMap{rounds: 2049}).Map(context.Background(), engine.Event{Value: input})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != string(expected[:]) {
		t.Fatal("hash result changed")
	}
}
