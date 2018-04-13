package badger

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/dgraph-io/badger"
	badgerStore "github.com/jaegertracing/jaeger/plugin/storage/badger/spanstore"
)

// Factory implements storage.Factory for Badger backend.
type Factory struct {
	Options *Options
	store   *badger.DB
	cache   *badgerStore.CacheStore

	tmpDir string

	cleaner func() error
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{
		Options: NewOptions("badger"),
	}
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	opts := badger.DefaultOptions

	if f.Options.primary.Ephemeral {
		opts.SyncWrites = false
		dir, err := ioutil.TempDir("", "badger")
		if err != nil {
			return err
		}
		f.tmpDir = dir
		opts.Dir = f.tmpDir
		opts.ValueDir = f.tmpDir
	} else {
		opts.SyncWrites = f.Options.primary.SyncWrites
		opts.Dir = f.Options.primary.KeyDirectory
		opts.ValueDir = f.Options.primary.ValueDirectory
	}

	store, err := badger.Open(opts)
	if err != nil {
		return err
	}
	f.store = store

	cache, err := badgerStore.NewCacheStore(f.store, f.Options.primary.SpanStoreTTL)
	if err != nil {
		return err
	}
	f.cache = cache

	f.cleaner = func() error {
		err := f.store.Close()
		if err != nil {
			return err
		}

		// Remove tmp files if this was ephemeral storage
		if f.Options.primary.Ephemeral {
			err = os.RemoveAll(f.tmpDir)
		}

		return err
	}

	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return badgerStore.NewTraceReader(f.store, f.cache), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return badgerStore.NewSpanWriter(f.store, f.cache, f.Options.primary.SpanStoreTTL, f.cleaner), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}
