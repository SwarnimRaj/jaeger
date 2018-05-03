package integration

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/jaegertracing/jaeger/pkg/testutils"
	"github.com/jaegertracing/jaeger/plugin/storage/badger"
	assert "github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)

type BadgerIntegrationStorage struct {
	StorageIntegration
}

func (s *BadgerIntegrationStorage) initialize() error {
	f := badger.NewFactory()
	err := f.Initialize(metrics.NullFactory, zap.NewNop())
	if err != nil {
		return err
	}

	sw, err := f.CreateSpanWriter()
	if err != nil {
		return err
	}
	sr, err := f.CreateSpanReader()
	if err != nil {
		return err
	}

	s.SpanReader = sr
	s.SpanWriter = sw

	s.Refresh = s.refresh
	s.CleanUp = s.cleanUp

	logger, _ := testutils.NewLogger()
	s.logger = logger

	return nil
}

func (s *BadgerIntegrationStorage) clear() error {
	if closer, ok := s.SpanWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("BadgerIntegrationStorage did not implement io.Closer, unable to close and cleanup the storage correctly")
}

func (s *BadgerIntegrationStorage) cleanUp() error {
	err := s.clear()
	if err != nil {
		return err
	}
	return s.initialize()
}

func (s *BadgerIntegrationStorage) refresh() error {
	return nil
}

func TestBadgerStorage(t *testing.T) {
	if os.Getenv("STORAGE") != "badger" {
		t.Skip("Integration test against Badger skipped; set STORAGE env var to badger to run this")
	}
	s := &BadgerIntegrationStorage{}
	assert.NoError(t, s.initialize())
	s.IntegrationTestAll(t)
	defer s.clear()
}
