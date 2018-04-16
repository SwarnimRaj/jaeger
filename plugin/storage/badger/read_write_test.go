package badger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	assert "github.com/stretchr/testify/require"
)

func TestWriteReadBack(t *testing.T) {
	runFactoryTest(t, func(t *testing.T, sw spanstore.Writer, sr spanstore.Reader) {
		tid := time.Now()
		traces := 40
		spans := 3
		for i := 0; i < traces; i++ {
			for j := 0; j < spans; j++ {
				s := model.Span{
					TraceID: model.TraceID{
						Low:  uint64(i),
						High: 1,
					},
					SpanID:        model.SpanID(j),
					OperationName: "operation",
					Process: &model.Process{
						ServiceName: "service",
					},
					StartTime: tid.Add(time.Duration(i)),
					Duration:  time.Duration(i + j),
				}
				err := sw.WriteSpan(&s)
				assert.NoError(t, err)
			}
		}

		for i := 0; i < traces; i++ {
			tr, err := sr.GetTrace(model.TraceID{
				Low:  uint64(i),
				High: 1,
			})
			assert.NoError(t, err)

			assert.Equal(t, spans, len(tr.Spans))
		}
	})
}

func TestFindValidation(t *testing.T) {
	runFactoryTest(t, func(t *testing.T, sw spanstore.Writer, sr spanstore.Reader) {
		tid := time.Now()
		params := &spanstore.TraceQueryParameters{
			StartTimeMin: tid,
			StartTimeMax: tid.Add(time.Duration(10)),
		}

		// Only StartTimeMin and Max (not supported yet)
		_, err := sr.FindTraces(params)
		assert.Error(t, err, errors.New("This query parameter is not supported yet"))

		params.OperationName = "no-service"
		_, err = sr.FindTraces(params)
		assert.Error(t, err, errors.New("Service Name must be set"))
	})
}

func TestIndexSeeks(t *testing.T) {
	runFactoryTest(t, func(t *testing.T, sw spanstore.Writer, sr spanstore.Reader) {
		startT := time.Now()
		traces := 20
		spans := 3
		tid := startT
		for i := 0; i < traces; i++ {
			tid = tid.Add(time.Duration(time.Millisecond * time.Duration(i)))
			for j := 0; j < spans; j++ {
				s := model.Span{
					TraceID: model.TraceID{
						Low:  uint64(i),
						High: 1,
					},
					SpanID:        model.SpanID(j),
					OperationName: fmt.Sprintf("operation-%d", j),
					Process: &model.Process{
						ServiceName: fmt.Sprintf("service-%d", i%4),
					},
					StartTime: tid,
					Duration:  time.Duration(i + j),
					Tags: model.KeyValues{
						model.KeyValue{
							Key:   fmt.Sprintf("k%d", i),
							VStr:  fmt.Sprintf("val%d", j),
							VType: model.StringType,
						},
					},
				}
				err := sw.WriteSpan(&s)
				assert.NoError(t, err)
			}
		}

		params := &spanstore.TraceQueryParameters{
			StartTimeMin: startT,
			StartTimeMax: startT.Add(time.Duration(time.Millisecond * 10)),
			ServiceName:  "service-1",
		}

		trs, err := sr.FindTraces(params)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(trs))

		params.OperationName = "operation-1"
		trs, err = sr.FindTraces(params)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(trs))

		params.ServiceName = "service-10" // this should not match
		trs, err = sr.FindTraces(params)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(trs))

		params.OperationName = "operation-4"
		trs, err = sr.FindTraces(params)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(trs))

		// Multi-index hits

		params.StartTimeMax = startT.Add(time.Duration(time.Millisecond * 66))
		params.ServiceName = "service-3"
		params.OperationName = "operation-1"
		tags := make(map[string]string)
		tags["k11"] = "val0"
		params.Tags = tags
		trs, err = sr.FindTraces(params)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(trs))
	})
}

/*
func TestMenuSeeks(t *testing.T) {
	runFactoryTest(t, func(t *testing.T, sw spanstore.Writer, sr spanstore.Reader) {
		tid := time.Now()
		traces := 40
		services := 4
		spans := 3
		for i := 0; i < traces; i++ {
			for j := 0; j < spans; j++ {
				s := model.Span{
					TraceID: model.TraceID{
						Low:  uint64(i),
						High: 1,
					},
					SpanID:        model.SpanID(j),
					OperationName: fmt.Sprintf("operation-%d", j),
					Process: &model.Process{
						ServiceName: fmt.Sprintf("service-%d", i%(traces%services)),
					},
					StartTime: tid.Add(time.Duration(i)),
					Duration:  time.Duration(i + j),
				}
				err := sw.WriteSpan(&s)
				assert.NoError(t, err)
			}
		}

		operations, err := sr.GetOperations("service-1")
		assert.NoError(t, err)

		serviceList, err := sr.GetServices()
		assert.NoError(t, err)

		assert.Equal(t, spans, len(operations))
		assert.Equal(t, services, len(serviceList))
	})
}
*/

// Opens a badger db and runs a a test on it.
func runFactoryTest(t *testing.T, test func(t *testing.T, sw spanstore.Writer, sr spanstore.Reader)) {
	f := NewFactory()
	// TODO Initialize with temporary directories which can be deleted afterwards

	/*
		dir, err := ioutil.TempDir("", "badger")
		assert.NoError(t, err)
	*/
	err := f.Initialize(metrics.NullFactory, zap.NewNop())
	assert.NoError(t, err)

	sw, err := f.CreateSpanWriter()
	assert.NoError(t, err)

	sr, err := f.CreateSpanReader()
	assert.NoError(t, err)

	defer func() {
		if closer, ok := sw.(io.Closer); ok {
			err := closer.Close()
			assert.NoError(t, err)
		} else {
			t.FailNow()
		}

		// os.RemoveAll(dir)
		os.RemoveAll("/tmp/badger")
	}()
	/*

		dir, err := ioutil.TempDir("", "badger")
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		if opts == nil {
			opts = new(Options)
			*opts = getTestOptions(dir)
		}
		db, err := Open(*opts)
		require.NoError(t, err)
		defer db.Close()
		test(t, db)
	*/

	test(t, sw, sr)
}
