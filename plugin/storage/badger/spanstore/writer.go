package spanstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/jaegertracing/jaeger/model"
)

/*
	This store should be easily modified to use any sorted KV-store, which allows set/get/iterators.
	That includes RocksDB also (this key structure should work as-is with RocksDB)

	Keys are written in BigEndian order to allow lexicographic sorting of keys
*/

const (
	primaryKeyPrefix      byte = 0x80 // All primary key values should have first bit set to 1
	indexKeyPrefix        byte = 0x00 // All secondary indexes should have first bit set to 0
	serviceNameIndexKey   byte = 0x01
	operationNameIndexKey byte = 0x02
	tagIndexKey           byte = 0x03
	durationIndexKey      byte = 0x04
	startTimeIndexKey     byte = 0x05 // Reserved
	jsonEncoding          byte = 0x01 // Last 4 bits of the meta byte are for encoding type
)

type SpanWriter struct {
	store *badger.DB
	ttl   uint64
	cache *CacheStore
}

func NewSpanWriter(db *badger.DB, c *CacheStore, ttl uint64) *SpanWriter {
	return &SpanWriter{
		store: db,
		ttl:   ttl,
		cache: c,
	}
}

// WriteSpan writes the encoded span as well as creates indexes with defined TTL
func (w *SpanWriter) WriteSpan(span *model.Span) error {

	// Avoid doing as much as possible inside the transaction boundary, create entries here
	entriesToStore := make([]*badger.Entry, 0, len(span.Tags)+4)

	trace, err := w.createTraceEntry(span)
	if err != nil {
		return err
	}

	entriesToStore = append(entriesToStore, trace)
	entriesToStore = append(entriesToStore, w.createBadgerEntry(createIndexKey(serviceNameIndexKey, []byte(span.Process.ServiceName), span.StartTime, span.TraceID), nil))
	entriesToStore = append(entriesToStore, w.createBadgerEntry(createIndexKey(operationNameIndexKey, []byte(span.Process.ServiceName+span.OperationName), span.StartTime, span.TraceID), nil))

	// It doesn't matter if we overwrite Duration index keys, everything is read at Trace level in any case
	durationValue := make([]byte, 8)
	binary.BigEndian.PutUint64(durationValue, uint64(model.DurationAsMicroseconds(span.Duration)))
	entriesToStore = append(entriesToStore, w.createBadgerEntry(createIndexKey(durationIndexKey, durationValue, span.StartTime, span.TraceID), nil))

	for _, kv := range span.Tags {
		// Ignore other types than String for now
		if kv.VType == model.StringType {
			// KEY: it<serviceName><tagsKey><traceId> VALUE: <tagsValue>
			entriesToStore = append(entriesToStore, w.createBadgerEntry(createIndexKey(tagIndexKey, []byte(span.Process.ServiceName+kv.Key+kv.VStr), span.StartTime, span.TraceID), nil))
		}
	}

	err = w.store.Update(func(txn *badger.Txn) error {
		// Write the entries
		for i := range entriesToStore {
			err = txn.SetEntry(entriesToStore[i])
			if err != nil {
				// Most likely primary key conflict, but let the caller check this
				return err
			}
		}

		// TODO Alternative option is to use simpler keys with the merge value interface. How this scales is a bit trickier
		// Fine after this is solved: https://github.com/dgraph-io/badger/issues/373

		return nil
	})

	// Do cache refresh here to release the transaction earlier
	w.cache.Update(span.Process.ServiceName, span.OperationName)

	return err
}

func createIndexKey(indexPrefixKey byte, value []byte, startTime time.Time, traceID model.TraceID) []byte {
	// KEY: indexKey<indexValue><startTime><traceId> (traceId is last 16 bytes of the key)
	buf := new(bytes.Buffer)

	buf.WriteByte(indexPrefixKey)
	buf.Write(value)
	binary.Write(buf, binary.BigEndian, model.TimeAsEpochMicroseconds(startTime))
	binary.Write(buf, binary.BigEndian, traceID.High)
	binary.Write(buf, binary.BigEndian, traceID.Low)
	return buf.Bytes()
}

func (w *SpanWriter) createBadgerEntry(key []byte, value []byte) *badger.Entry {
	return &badger.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: uint64(time.Now().Add(time.Hour * time.Duration(w.ttl)).Unix()),
	}
}

func (w *SpanWriter) createTraceEntry(span *model.Span) (*badger.Entry, error) {
	pK, pV, err := createTraceKV(span)
	if err != nil {
		return nil, err
	}

	e := w.createBadgerEntry(pK, pV)
	e.UserMeta = jsonEncoding

	return e, nil
}

func createTraceKV(span *model.Span) ([]byte, []byte, error) {
	// TODO Add Hash for Zipkin compatibility?

	// Note, KEY must include startTime for proper sorting order for span-ids
	// KEY: ti<trace-id><startTime><span-id> VALUE: All the details (json for now) METADATA: Encoding
	buf := new(bytes.Buffer)

	buf.WriteByte(primaryKeyPrefix)
	binary.Write(buf, binary.BigEndian, span.TraceID.High)
	binary.Write(buf, binary.BigEndian, span.TraceID.Low)
	binary.Write(buf, binary.BigEndian, model.TimeAsEpochMicroseconds(span.StartTime))
	binary.Write(buf, binary.BigEndian, span.SpanID)

	jb, err := json.Marshal(span)

	return buf.Bytes(), jb, err
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	return w.store.Close()
}
