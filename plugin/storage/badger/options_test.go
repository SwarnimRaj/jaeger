package badger

import (
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/pkg/config"
	assert "github.com/stretchr/testify/require"
)

func TestDefaultOptionsParsing(t *testing.T) {
	opts := NewOptions("badger")
	v, command := config.Viperize(opts.AddFlags)
	command.ParseFlags([]string{})
	opts.InitFromViper(v)

	assert.True(t, opts.GetPrimary().Ephemeral)
	assert.False(t, opts.GetPrimary().SyncWrites)
	assert.Equal(t, time.Duration(72*time.Hour), opts.GetPrimary().SpanStoreTTL)
}

func TestParseOptions(t *testing.T) {
	opts := NewOptions("badger")
	v, command := config.Viperize(opts.AddFlags)
	command.ParseFlags([]string{
		"--badger.ephemeral=false",
		"--badger.consistency=true",
		"--badger.directory-key=/var/lib/badger",
		"--badger.directory-value=/mnt/slow/badger",
		"--badger.span-store-ttl=168h",
	})
	opts.InitFromViper(v)

	assert.False(t, opts.GetPrimary().Ephemeral)
	assert.True(t, opts.GetPrimary().SyncWrites)
	assert.Equal(t, time.Duration(168*time.Hour), opts.GetPrimary().SpanStoreTTL)
	assert.Equal(t, "/var/lib/badger", opts.GetPrimary().KeyDirectory)
	assert.Equal(t, "/mnt/slow/badger", opts.GetPrimary().ValueDirectory)
}
