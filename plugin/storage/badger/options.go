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
	"flag"
	"time"

	"github.com/spf13/viper"
)

type Options struct {
	primary *namespaceConfig
	others  map[string]*namespaceConfig
}

type namespaceConfig struct {
	namespace      string
	SpanStoreTTL   time.Duration
	ValueDirectory string
	KeyDirectory   string
	Ephemeral      bool // Setting this to true will ignore ValueDirectory and KeyDirectory
	SyncWrites     bool
}

const (
	suffixKeyDirectory   = ".directory-key"
	suffixValueDirectory = ".directory-value"
	suffixEphemeral      = ".ephemeral"
	suffixSpanstoreTTL   = ".span-store-ttl"
	suffixSyncWrite      = ".consistency"
)

// NewOptions creates a new Options struct.
func NewOptions(primaryNamespace string, otherNamespaces ...string) *Options {
	options := &Options{
		primary: &namespaceConfig{
			namespace:      primaryNamespace,
			SpanStoreTTL:   time.Hour * 72, // Default is 3 days
			SyncWrites:     false,          // Performance over consistency
			Ephemeral:      true,           // Default is ephemeral storage
			ValueDirectory: "",
			KeyDirectory:   "",
		},
		others: make(map[string]*namespaceConfig, len(otherNamespaces)),
	}

	for _, namespace := range otherNamespaces {
		options.others[namespace] = &namespaceConfig{namespace: namespace}
	}

	return options
}

// AddFlags adds flags for Options
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	addFlags(flagSet, opt.primary)
	for _, cfg := range opt.others {
		addFlags(flagSet, cfg)
	}
}

func addFlags(flagSet *flag.FlagSet, nsConfig *namespaceConfig) {
	flagSet.Bool(
		nsConfig.namespace+suffixEphemeral,
		nsConfig.Ephemeral,
		"Mark this storage ephemeral, data is stored in tmpfs (in-memory). Default is true.",
	)
	flagSet.Duration(
		nsConfig.namespace+suffixSpanstoreTTL,
		nsConfig.SpanStoreTTL,
		"time.Duration to store the data. Default is 72 hours (3 days).",
	)
	flagSet.String(
		nsConfig.namespace+suffixKeyDirectory,
		nsConfig.KeyDirectory,
		"Path to store the keys (indexes), this directory should reside in SSD disk. Set ephmeral to false if you want to define this setting.",
	)
	flagSet.String(
		nsConfig.namespace+suffixValueDirectory,
		nsConfig.ValueDirectory,
		"Path to store the values (spans). Set ephmeral to false if you want to define this setting.",
	)
	flagSet.Bool(
		nsConfig.namespace+suffixSyncWrite,
		nsConfig.SyncWrites,
		"If all writes should be synced immediately. This will greatly reduce write performance and will require fast SSD drives. Default is false.",
	)
}

// InitFromViper initializes Options with properties from viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	initFromViper(opt.primary, v)
	for _, cfg := range opt.others {
		initFromViper(cfg, v)
	}
}

func initFromViper(cfg *namespaceConfig, v *viper.Viper) {
	cfg.Ephemeral = v.GetBool(cfg.namespace + suffixEphemeral)
	cfg.KeyDirectory = v.GetString(cfg.namespace + suffixKeyDirectory)
	cfg.ValueDirectory = v.GetString(cfg.namespace + suffixValueDirectory)
	cfg.SyncWrites = v.GetBool(cfg.namespace + suffixSyncWrite)
	cfg.SpanStoreTTL = v.GetDuration(cfg.namespace + suffixSpanstoreTTL)
}

// GetPrimary returns the primary namespace configuration
func (opt *Options) GetPrimary() *namespaceConfig {
	return opt.primary
}
