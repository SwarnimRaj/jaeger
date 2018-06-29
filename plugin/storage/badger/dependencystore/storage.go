package dependencystore

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
)

const (
	dependencyKeyPrefix byte = 0xC0 // Dependency PKs have first two bits set to 1
)

// DependencyStore handles all queries and insertions to Cassandra dependencies
type DependencyStore struct {
	// session                  cassandra.Session
	// dependencyDataFrequency  time.Duration
	// dependenciesTableMetrics *casMetrics.Table
	// logger                   *zap.Logger
}

// NewDependencyStore returns a DependencyStore
func NewDependencyStore() *DependencyStore {
	// 	session cassandra.Session,
	// 	dependencyDataFrequency time.Duration,
	// 	metricsFactory metrics.Factory,
	// 	logger *zap.Logger,
	// ) *DependencyStore {
	// 	return &DependencyStore{
	// 		session:                  session,
	// 		dependencyDataFrequency:  dependencyDataFrequency,
	// 		dependenciesTableMetrics: casMetrics.NewTable(metricsFactory, "Dependencies"),
	// 		logger: logger,
	// 	}
	return nil
}

// WriteDependencies implements dependencystore.Writer#WriteDependencies.
func (s *DependencyStore) WriteDependencies(ts time.Time, dependencies []model.DependencyLink) error {
	// deps := make([]Dependency, len(dependencies))
	// for i, d := range dependencies {
	// 	deps[i] = Dependency{
	// 		Parent:    d.Parent,
	// 		Child:     d.Child,
	// 		CallCount: int64(d.CallCount),
	// 	}
	// }
	// query := s.session.Query(depsInsertStmt, ts, ts, deps)
	// return s.dependenciesTableMetrics.Exec(query, s.logger)
	return nil
}

// GetDependencies returns all interservice dependencies
func (s *DependencyStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	// query := s.session.Query(depsSelectStmt, endTs.Add(-1*lookback), endTs)
	// iter := query.Consistency(cassandra.One).Iter()

	// var mDependency []model.DependencyLink
	// var dependencies []Dependency
	// var ts time.Time
	// for iter.Scan(&ts, &dependencies) {
	// 	for _, dependency := range dependencies {
	// 		mDependency = append(mDependency, model.DependencyLink{
	// 			Parent:    dependency.Parent,
	// 			Child:     dependency.Child,
	// 			CallCount: uint64(dependency.CallCount),
	// 		})
	// 	}
	// }

	// if err := iter.Close(); err != nil {
	// 	s.logger.Error("Failure to read Dependencies", zap.Time("endTs", endTs), zap.Duration("lookback", lookback), zap.Error(err))
	// 	return nil, errors.Wrap(err, "Error reading dependencies from storage")
	// }
	// return mDependency, nil
	return nil, nil
}
