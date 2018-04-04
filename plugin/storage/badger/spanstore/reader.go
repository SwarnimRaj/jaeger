package spanstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// All of this is replicated from the ES and Cassandra storage parts.. they should be refactored to common place

var (
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("Service Name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("Start Time Minimum is above Maximum")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("Duration Minimum is above Maximum")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("Malformed request object")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("Start and End Time must be set")

	// ErrUnableToFindTraceIDAggregation occurs when an aggregation query for TraceIDs fail.
	ErrUnableToFindTraceIDAggregation = errors.New("Could not find aggregation of traceIDs")

	// ErrNotSupported during development, don't support every option - yet
	ErrNotSupported = errors.New("This query parameter is not supported yet")

	errNoTraces = errors.New("No trace with that ID found")

	defaultMaxDuration = model.DurationAsMicroseconds(time.Hour * 24)
)

const (
	defaultNumTraces = 100
)

type TraceReader struct {
	store *badger.DB
	cache *CacheStore
}

func NewTraceReader(db *badger.DB, c *CacheStore) *TraceReader {
	return &TraceReader{
		store: db,
		cache: c,
	}
}

func (r *TraceReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	// Get by PK
	spans := make([]*model.Span, 0)

	err := r.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := createPrimaryKeySeekPrefix(traceID)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Add value to the span store (decode from JSON / defined encoding first)
			// These are in the correct order because of the sorted nature
			item := it.Item()
			val, err := item.Value()

			if err != nil {
				return err
			}
			sp := model.Span{}
			err = json.Unmarshal(val, &sp)
			if err != nil {
				return err
			}
			spans = append(spans, &sp)
		}
		return nil
	})

	// TODO Do the Unmarshal here so we can release the transaction

	trace := &model.Trace{
		Spans: spans,
	}

	return trace, err
}

func createPrimaryKeySeekPrefix(traceID model.TraceID) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(primaryKeyPrefix)
	binary.Write(buf, binary.BigEndian, traceID.High)
	binary.Write(buf, binary.BigEndian, traceID.Low)
	return buf.Bytes()
}

// GetServices fetches the sorted service list that have not expired
func (r *TraceReader) GetServices() ([]string, error) {
	return r.cache.GetServices()
}

// GetOperations fetches operations in the service and empty slice if service does not exists
func (r *TraceReader) GetOperations(service string) ([]string, error) {
	return r.cache.GetOperations(service)
}

func (r *TraceReader) FindTraces(query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {

	// TODO Optimize these queries by using adaptive range query filters to avoid unnecessarily large queries for the set operations
	// Improves scalability for the read path
	err := validateQuery(query)
	if err != nil {
		return nil, err
	}

	if query.NumTraces == 0 {
		query.NumTraces = defaultNumTraces
	}

	indexSeeks := make([][]byte, 0, 1)

	if query.ServiceName != "" {
		indexSearchKey := make([]byte, 0, 64) // 64 is a magic guess
		if query.OperationName != "" {
			indexSearchKey = append(indexSearchKey, operationNameIndexKey)
			indexSearchKey = append(indexSearchKey, []byte(query.ServiceName+query.OperationName)...)
		} else {
			indexSearchKey = append(indexSearchKey, serviceNameIndexKey)
			indexSearchKey = append(indexSearchKey, []byte(query.ServiceName)...)
		}

		indexSeeks = append(indexSeeks, indexSearchKey)
		if len(query.Tags) > 0 {
			for k, v := range query.Tags {
				tagSearch := []byte(query.ServiceName + k + v)
				tagSearchKey := make([]byte, 0, len(tagSearch)+1)
				tagSearchKey = append(tagSearchKey, tagIndexKey)
				tagSearchKey = append(tagSearchKey, tagSearch...)
				indexSeeks = append(indexSeeks, tagSearchKey)
			}
		}
	}

	// TODO DurationIndex scanning (it is range index scanning - not exact seek)

	// Get intersection of all the hits.. optimization problem without distribution knowledge: would it make more sense to scan the primary keys and decode them instead?
	// Key only scan is a lot faster in the badger
	if len(indexSeeks) > 0 {
		ids := make([][][]byte, 0, len(indexSeeks))
		for i, s := range indexSeeks {
			indexResults, _ := r.scanIndexKeys(s, query.StartTimeMin, query.StartTimeMax)
			if err != nil {
				return nil, err
			}
			ids = append(ids, make([][]byte, 0, len(indexResults)))
			for _, k := range indexResults {
				ids[i] = append(ids[i], k[len(k)-16:])
			}
		}

		intersected := ids[0]
		mergeIntersected := make([][]byte, 0, len(intersected)) // intersected is the maximum size

		for i := 1; i < len(ids); i++ {
			k := len(intersected) - 1
			for j := len(ids[i]) - 1; j >= 0 && k >= 0; {
				// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
				switch bytes.Compare(intersected[k], ids[i][j]) {
				case 1:
					k-- // Move on to the next item in the intersected list
					// a > b
				case -1:
					j--
					// a < b
					// Move on to next iteration of j
				case 0:
					mergeIntersected = append(mergeIntersected, intersected[k])
					k-- // Move on to next item
					// Match
				}
			}
			intersected = mergeIntersected
			mergeIntersected = make([][]byte, 0, len(intersected)) // intersected is the maximum size
		}

		// Get top query.NumTraces results (note, the slice is now in descending timestamp order)
		if query.NumTraces < len(intersected) {
			intersected = intersected[:query.NumTraces-1]
		}

		// Enrich the traceIds to model.Trace
		result := make([]*model.Trace, 0, len(intersected))
		for _, key := range intersected {
			// TODO This is a slow way to do the read, since each of these calls starts a new transaction
			// Done for the PoC only
			tr, err := r.GetTrace(
				model.TraceID{
					High: binary.BigEndian.Uint64(key[:8]),
					Low:  binary.BigEndian.Uint64(key[8:]),
				},
			)
			if err != nil {
				return nil, err
			}
			result = append(result, tr)
		}

		return result, nil
	}
	// We need to do a primary key scan and check all the TraceIDs if they match certain time range (full table scan basically unless we add index for it)
	return nil, ErrNotSupported
}

func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}

	if p.ServiceName == "" && p.OperationName != "" {
		return ErrServiceNameNotSet
	}

	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}

	if !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	return nil
}

// scanIndexKeys scans the time range for index keys matching the given prefix.
func (r *TraceReader) scanIndexKeys(indexKeyValue []byte, startTimeMin time.Time, startTimeMax time.Time) ([][]byte, error) {
	indexResults := make([][]byte, 0)

	startStampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(startStampBytes, model.TimeAsEpochMicroseconds(startTimeMin))

	err := r.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Don't fetch values since we're only interested in the keys
		it := txn.NewIterator(opts)
		defer it.Close()

		// Create starting point for sorted index scan
		startIndex := make([]byte, 0, len(indexKeyValue)+len(startStampBytes))
		startIndex = append(startIndex, indexKeyValue...)
		startIndex = append(startIndex, startStampBytes...)

		for it.Seek(startIndex); scanFunction(it, indexKeyValue, model.TimeAsEpochMicroseconds(startTimeMax)); it.Next() {
			item := it.Item()

			// ScanFunction is a prefix scanning (since we could have for example service1 & service12)
			// Now we need to match only the exact key if we want to add it
			timestampStartIndex := len(it.Item().Key()) - 24
			if bytes.Compare(indexKeyValue, it.Item().Key()[:timestampStartIndex]) == 0 {
				key := []byte{}
				key = append(key, item.Key()...) // badger reuses underlying slices so we have to copy the key
				indexResults = append(indexResults, key)
			}
		}
		return nil
	})
	return indexResults, err
}

// scanFunction compares the index name as well as the time range in the index key
func scanFunction(it *badger.Iterator, indexPrefix []byte, timeIndexEnd uint64) bool {
	if it.Item() != nil {
		// We can't use the indexPrefix length, because we might have the same prefixValue for non-matching cases also
		timestampStartIndex := len(it.Item().Key()) - 24
		timestamp := binary.BigEndian.Uint64(it.Item().Key()[timestampStartIndex : timestampStartIndex+8])

		return bytes.HasPrefix(it.Item().Key()[:timestampStartIndex], indexPrefix) && timestamp <= timeIndexEnd
	}
	return false
}

// Close Implements io.Closer
func (w *TraceReader) Close() error {
	// Allows this to be signaled that we've been closed
	return nil
}
