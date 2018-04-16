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
)

type SpanWriter struct {
	store *badger.DB
	ttl   uint64
}

func NewSpanWriter(s *badger.DB, ttl uint64) *SpanWriter {
	return &SpanWriter{
		store: s,
		ttl:   ttl,
	}
}

// WriteSpan writes the encoded span as well as creates indexes with defined TTL
func (w *SpanWriter) WriteSpan(span *model.Span) error {
	// TODO Store with Metadata (TTL + encoding)
	// TODO Txn.SetWithMeta() would allow to indicate for example JSON / Protobuf / compression / etc encoding of the value
	err := w.store.Update(func(txn *badger.Txn) error {
		// Write the primary key with a value
		pkK, pkV, err := createTraceKV(span)
		if err != nil {
			return err
		}
		err = txn.Set(pkK, pkV)
		if err != nil {
			// Most likely primary key conflict, but let the caller check this
			return err
		}

		// TODO Optimization: Posting List with time bucketing (using a merge function), insersect to get correct answers
		// Create the index keys - we use index keys only as they're faster to lookup than fetching the values (and do not require conflict handling)
		txn.Set(createIndexKey(serviceNameIndexKey, []byte(span.Process.ServiceName), span.StartTime, span.TraceID), nil)
		// This has to use a different index name than the serviceName, imagine: operation "1", service: "service" vs service: "service1"
		txn.Set(createIndexKey(operationNameIndexKey, []byte(span.Process.ServiceName+span.OperationName), span.StartTime, span.TraceID), nil)

		// It doesn't matter if we overwrite Duration index keys, everything is read at Trace level in any case
		durationValue := make([]byte, 8)
		binary.BigEndian.PutUint64(durationValue, uint64(span.Duration))
		txn.Set(createIndexKey(durationIndexKey, durationValue, span.StartTime, span.TraceID), nil)

		// TODO Alternative option is to use simpler keys with the merge value interface. How this scales is a bit trickier
		// Fine after this is solved: https://github.com/dgraph-io/badger/issues/373

		for _, kv := range span.Tags {
			// Ignore other types than String for now
			if kv.VType == model.StringType {
				// KEY: it<serviceName><tagsKey><traceId> VALUE: <tagsValue>
				txn.Set(createIndexKey(tagIndexKey, []byte(span.Process.ServiceName+kv.Key+kv.VStr), span.StartTime, span.TraceID), nil)
			}
		}

		return nil
	})
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

func createTraceKV(span *model.Span) ([]byte, []byte, error) {
	// This key is bad for fetching a single traceId as it needs to find the next position (we don't know all the starttimes)
	// Should probably use a merge function to store all the traces under a single key
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
