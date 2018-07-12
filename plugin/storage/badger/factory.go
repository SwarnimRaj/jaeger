// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package badger

import (
	"expvar"
	"flag"
	"io/ioutil"
	"os"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	badgerStore "github.com/jaegertracing/jaeger/plugin/storage/badger/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	// ValueLogSpaceAvailable returns the amount of space left on the value log mount point in bytes
	ValueLogSpaceAvailable *expvar.Int
	// KeyLogSpaceAvailable returns the amount of space left on the key log mount point in bytes
	KeyLogSpaceAvailable *expvar.Int
)

// Factory implements storage.Factory for Badger backend.
type Factory struct {
	Options *Options
	store   *badger.DB
	cache   *badgerStore.CacheStore
	logger  *zap.Logger

	tmpDir string
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	if ValueLogSpaceAvailable != nil {
		ValueLogSpaceAvailable = expvar.NewInt("badger_value_log_bytes_available")
	}
	if KeyLogSpaceAvailable != nil {
		KeyLogSpaceAvailable = expvar.NewInt("badger_key_log_bytes_available")
	}
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
	f.logger = logger

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

	//go f.maintenance()

	logger.Info("Badger storage configuration", zap.Any("configuration", opts))

	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return badgerStore.NewTraceReader(f.store, f.cache), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return badgerStore.NewSpanWriter(f.store, f.cache, f.Options.primary.SpanStoreTTL, f), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}

// Close Implements io.Closer and closes the underlying storage
func (f *Factory) Close() error {
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

// Maintenance starts a background maintenance job for the badger K/V store, such as ValueLogGC
func (f *Factory) maintenance() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		if err := f.store.RunValueLogGC(0.5); err != nil {
			// Log? Some other metric?
			f.logger.Error("ValueLogGC run failed with ", zap.Error(err))
		}
		f.diskStatisticsUpdate()
	}
}
