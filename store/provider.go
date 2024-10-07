package store

import (
	"io"
	"time"

	"github.com/tarungka/wire/internal/command/proto"
)

// Provider implements the uploader Provider interface, allowing the
// Store to be used as a DataProvider for an uploader.
type Provider struct {
	str      *Store
	vacuum   bool
	compress bool

	nRetries      int
	retryInterval time.Duration
}

// NewProvider returns a new instance of Provider. If v is true, the
// SQLite database will be VACUUMed before being provided. If c is
// true, the SQLite database will be compressed before being provided.
func NewProvider(s *Store, v, c bool) *Provider {
	return &Provider{
		str:           s,
		vacuum:        v,
		compress:      c,
		nRetries:      10,
		retryInterval: 500 * time.Millisecond,
	}
}

// LastIndex returns the cluster-wide index the data managed by the DataProvider was
// last modified by.
func (p *Provider) LastIndex() (uint64, error) {
	stats.Add(numProviderChecks, 1)
	return p.str.DBAppliedIndex(), nil
}

// Provider writes the SQLite database to the given path. If path exists,
// it will be overwritten.
func (p *Provider) Provide(w io.Writer) (retErr error) {
	stats.Add(numProviderProvides, 1)
	defer func() {
		if retErr != nil {
			stats.Add(numProviderProvidesFail, 1)
		}
	}()

	br := &proto.BackupRequest{
		Format:   proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Vacuum:   p.vacuum,
		Compress: p.compress,
	}
	nRetries := 0
	for {
		err := p.str.Backup(br, w)
		if err == nil {
			break
		}
		time.Sleep(p.retryInterval)
		nRetries++
		if nRetries > p.nRetries {
			return err
		}
	}
	return nil
}
