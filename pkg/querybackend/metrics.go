package querybackend

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	datasetTenantIsolationFailure prometheus.Counter

	// symbolRefUnresolvedCapExceeded counts locations that exceeded
	// WithResolverSymbolRefCap and were rendered as an inline fallback name
	// instead of being interned as a deferred unresolved entry.
	symbolRefUnresolvedCapExceeded prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		datasetTenantIsolationFailure: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "dataset_tenant_isolation_failure_total",
			}),
		symbolRefUnresolvedCapExceeded: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "symbol_ref_unresolved_cap_exceeded_total",
				Help:      "Number of locations rendered as an inline fallback name after exceeding the unresolved symbol ref cap.",
			}),
	}
	if reg != nil {
		reg.MustRegister(m.datasetTenantIsolationFailure)
		reg.MustRegister(m.symbolRefUnresolvedCapExceeded)
	}
	return m
}
