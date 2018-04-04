package badger

import (
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/dgraph-io/badger"
	badgerStore "github.com/jaegertracing/jaeger/plugin/storage/badger/spanstore"
)

const (
	defaultTTL uint64 = 168 // 7 days
)

// Factory implements storage.Factory for Badger backend.
type Factory struct {
	// Add ValueDir & Dir properties (where to store the database)
	// SyncWrites=false as default (this is local storage after all), allow override
	store *badger.DB
	cache *badgerStore.CacheStore

	ttl        uint64 // Hours
	storageDir string
	safeSync   bool
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

	f.ttl = defaultTTL // TODO Make configurable TTL

	store, err := badger.Open(opts)
	if err != nil {
		return err
	}
	f.store = store

	cache, err := badgerStore.NewCacheStore(f.store, f.ttl)
	if err != nil {
		return err
	}
	f.cache = cache

	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return badgerStore.NewTraceReader(f.store, f.cache), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return badgerStore.NewSpanWriter(f.store, f.cache, f.ttl), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}
