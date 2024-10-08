package snapshot

import (
	"io"
	"log"

	"github.com/hashicorp/raft"
)

// Snapshot represents a snapshot of the database state.
type Snapshot struct {
	rc     io.ReadCloser
	logger *log.Logger
}

// NewSnapshot creates a new snapshot.
func NewSnapshot(rc io.ReadCloser) *Snapshot {
	return &Snapshot{
		rc:     rc,
		logger: log.New(log.Writer(), "[snapshot] ", log.LstdFlags),
	}
}


// TODO: need to impl this
// Persist writes the snapshot to the given sink.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	defer s.rc.Close()
	// startT := time.Now()

	// cw := progress.NewCountingWriter(sink)
	// cm := progress.StartCountingMonitor(func(n int64) {
	// 	s.logger.Printf("persisted %d bytes", n)
	// }, cw)
	// n, err := func() (int64, error) {
	// 	defer cm.StopAndWait()
	// 	return io.Copy(cw, s.rc)
	// }()
	// if err != nil {
	// 	return err
	// }

	// dur := time.Since(startT)
	// stats.Get(persistSize).(*expvar.Int).Set(n)
	// stats.Get(persistDuration).(*expvar.Int).Set(dur.Milliseconds())
	// return err
	return nil
}

// Release releases the snapshot.
func (s *Snapshot) Release() {
	// Ensure that the source data for the snapshot is closed regardless of
	// whether the snapshot is persisted or not.
	s.rc.Close()
}
