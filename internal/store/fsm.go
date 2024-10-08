package store

import (
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// FSM is a wrapper around the Store which implements raft.FSM.
type FSM struct {
	s *Store
}

// NewFSM returns a new FSM.
func NewFSM(s *Store) *FSM {
	return &FSM{s: s}
}

// Apply applies a Raft log entry to the Store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	return f.s.fsmApply(l)
}

// Snapshot returns a Snapshot of the Store
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.s.fsmSnapshot()
}

// Restore restores the Store from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	return f.s.fsmRestore(rc)
}

// FSMSnapshot is a wrapper around raft.FSMSnapshot which adds an optional
// Finalizer, instrumentation, and logging.
type FSMSnapshot struct {
	Finalizer func() error
	OnFailure func()

	raft.FSMSnapshot
	persistSucceeded bool
	// TODO: change this to zerolog
	logger           *log.Logger
}

// Persist writes the snapshot to the given sink.
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) (retError error) {
	startT := time.Now()
	defer func() {
		if retError == nil {
			f.persistSucceeded = true
			dur := time.Since(startT)
			// TODO: add snapshots
			// stats.Add(numSnapshotPersists, 1)
			// stats.Get(snapshotPersistDuration).(*expvar.Int).Set(dur.Milliseconds())
			if f.logger != nil {
				f.logger.Printf("persisted snapshot %s in %s", sink.ID(), dur)
			}
		} else {
			// TODO: add snapshots
			// stats.Add(numSnapshotPersistsFailed, 1)
			if f.logger != nil {
				f.logger.Printf("failed to persist snapshot %s: %v", sink.ID(), retError)
			}
		}
	}()
	if err := f.FSMSnapshot.Persist(sink); err != nil {
		return err
	}
	if f.Finalizer != nil {
		return f.Finalizer()
	}
	return nil
}

// Release performs any final cleanup once the Snapshot has been persisted.
func (f *FSMSnapshot) Release() {
	f.FSMSnapshot.Release()
	if !f.persistSucceeded && f.OnFailure != nil {
		f.OnFailure()
	}
}

// TODO: incomplete implementation
// fsmApply applies a Raft log entry to the database.
func (s *Store) fsmApply(l *raft.Log) (e interface{}) {
	defer func() {
		s.fsmIdx.Store(l.Index)
		s.fsmTarget.Signal(l.Index)
		s.fsmTerm.Store(l.Term)
		s.fsmUpdateTime.Store(time.Now())
		s.appendedAtTime.Store(l.AppendedAt)
	}()

	if s.firstLogAppliedT.IsZero() {
		s.firstLogAppliedT = time.Now()
		s.logger.Printf("first log applied since node start, log at index %d", l.Index)
	}

	// cmd, mutated, r := s.cmdProc.Process(l.Data, s.db)
	// if mutated {
	// 	s.dbAppliedIdx.Store(l.Index)
	// 	s.appliedTarget.Signal(l.Index)
	// }
	// if cmd.Type == proto.Command_COMMAND_TYPE_NOOP {
	// 	s.numNoops.Add(1)
	// } else if cmd.Type == proto.Command_COMMAND_TYPE_LOAD {
	// 	// Swapping in a new database invalidates any existing snapshot.
	// 	if err := s.snapshotStore.SetFullNeeded(); err != nil {
	// 		s.logger.Fatalf("failed to set full snapshot needed: %s", err)
	// 	}
	// }
	return nil
}

// fsmSnapshot returns a snapshot of the database.
//
// Hashicorp Raft guarantees that this function will not be called concurrently
// with Apply, as it states Apply() and Snapshot() are always called from the same
// thread.
func (s *Store) fsmSnapshot() (fSnap raft.FSMSnapshot, retErr error) {
	if !s.open.Is() {
		return nil, ErrNotOpen
	}

	return nil, nil
}


// TODO: implementation is not complete
// fsmRestore restores the node to a previous state. The Hashicorp docs state this
// will not be called concurrently with Apply(), so synchronization with Execute()
// is not necessary.
func (s *Store) fsmRestore(rc io.ReadCloser) (retErr error) {
	defer func() {
		if retErr != nil {
			// stats.Add(numRestoresFailed, 1)
		}
	}()
	s.logger.Printf("initiating node restore on node ID %s", s.raftID)

	return nil

	// startT := time.Now()
	// // Create a scatch file to write the restore data to.
	// tmpFile, err := createTemp(s.dbDir, restoreScratchPattern)
	// if err != nil {
	// 	return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	// }
	// defer os.Remove(tmpFile.Name())
	// defer tmpFile.Close()

	// // Copy it from the reader to the temporary file.
	// _, err = io.Copy(tmpFile, rc)
	// if err != nil {
	// 	return fmt.Errorf("error copying restore data: %v", err)
	// }
	// if err := tmpFile.Close(); err != nil {
	// 	return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	// }

	// if err := s.db.Swap(tmpFile.Name(), s.dbConf.FKConstraints, true); err != nil {
	// 	return fmt.Errorf("error swapping database file: %v", err)
	// }
	// s.logger.Printf("successfully opened database at %s due to restore", s.db.Path())

	// // Take conservative approach and assume that everything has changed, so update
	// // the indexes. It is possible that dbAppliedIdx is now ahead of some other nodes'
	// // same value, since the last index is not necessarily a database-changing index,
	// // but that is OK. Worse that can happen is that anything paying attention to the
	// // index might consider the database to be changed when it is not, *logically* speaking.
	// li, tm, err := snapshot.LatestIndexTerm(s.snapshotDir)
	// if err != nil {
	// 	return fmt.Errorf("failed to get latest snapshot index post restore: %s", err)
	// }
	// s.fsmIdx.Store(li)
	// s.fsmTarget.Signal(li)
	// s.fsmTerm.Store(tm)
	// s.dbAppliedIdx.Store(li)
	// s.appliedTarget.Signal(li)
	// lt, err := s.db.DBLastModified()
	// if err != nil {
	// 	return fmt.Errorf("failed to get last modified time: %s", err)
	// }
	// s.dbModifiedTime.Store(lt)

	// stats.Add(numRestores, 1)
	// s.logger.Printf("node restored in %s", time.Since(startT))
	// rc.Close()
	// return nil
}