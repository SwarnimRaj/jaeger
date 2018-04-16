package badger

import (
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/dgraph-io/badger"
	badgerStore "github.com/jaegertracing/jaeger/plugin/storage/badger/spanstore"
)

// Factory implements storage.Factory for Badger backend.
type Factory struct {
	// Add TTL property
	// Add ValueDir & Dir properties (where to store the database)
	// SyncWrites=false as default (this is local storage after all), allow override
	store *badger.DB
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{}
}

// TODO Implement plugin.Configuration to allow proper configuration

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	opts := badger.DefaultOptions
	// TODO Temporary for testing purposes
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"

	store, err := badger.Open(opts)
	if err != nil {
		return err
	}
	f.store = store
	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return badgerStore.NewTraceReader(f.store), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return badgerStore.NewSpanWriter(f.store, 60000), nil // TODO Get real TTL parsed here
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}
