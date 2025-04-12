package aggregate

import (
	"hash/maphash"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var maphashSeed = maphash.MakeSeed()

type resourceKey pcommon.Resource

func (k resourceKey) Hash() uint64 {
	return pdatautil.Hash64(pdatautil.WithMap(pcommon.Resource(k).Attributes()))
}

func (k resourceKey) Equal(other resourceKey) bool {
	return pcommon.Resource(k).Attributes().Equal(pcommon.Resource(other).Attributes())
}

type scopeKey pcommon.InstrumentationScope

func (k scopeKey) Hash() uint64 {
	scope := pcommon.InstrumentationScope(k)
	return pdatautil.Hash64(
		pdatautil.WithString(scope.Name()),
		pdatautil.WithString(scope.Version()),
		pdatautil.WithMap(scope.Attributes()),
	)
}

func (k scopeKey) Equal(other scopeKey) bool {
	scope := pcommon.InstrumentationScope(k)
	otherScope := pcommon.InstrumentationScope(other)
	return scope.Name() == otherScope.Name() &&
		scope.Version() == otherScope.Version() &&
		scope.Attributes().Equal(otherScope.Attributes())
}

type metricKey struct {
	Type                   pmetric.MetricType
	Name                   string
	Unit                   string
	AggregationTemporality pmetric.AggregationTemporality
	IsMonotonic            bool
}

func newMetricKey(metric pmetric.Metric) metricKey {
	key := metricKey{
		Type: metric.Type(),
		Name: metric.Name(),
		Unit: metric.Unit(),
	}
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
	case pmetric.MetricTypeSum:
		key.AggregationTemporality = metric.Sum().AggregationTemporality()
		key.IsMonotonic = metric.Sum().IsMonotonic()
	case pmetric.MetricTypeHistogram:
		key.AggregationTemporality = metric.Histogram().AggregationTemporality()
	case pmetric.MetricTypeExponentialHistogram:
		key.AggregationTemporality = metric.ExponentialHistogram().AggregationTemporality()
	}
	return key
}

func (k metricKey) Hash() uint64 {
	return maphash.Comparable(maphashSeed, k)
}

func (k metricKey) Equal(other metricKey) bool {
	return k == other
}

type numberDataPointKey struct {
	StartTimestamp pcommon.Timestamp
	Attributes     pcommon.Map
	ValueType      pmetric.NumberDataPointValueType
}

func newNumberDataPointKey(dp pmetric.NumberDataPoint) numberDataPointKey {
	return numberDataPointKey{
		StartTimestamp: dp.StartTimestamp(),
		Attributes:     dp.Attributes(),
		ValueType:      dp.ValueType(),
	}
}

func (k numberDataPointKey) Hash() uint64 {
	var h maphash.Hash
	h.SetSeed(maphashSeed)
	maphash.WriteComparable(&h, k.StartTimestamp)
	maphash.WriteComparable(&h, pdatautil.MapHash(k.Attributes))
	maphash.WriteComparable(&h, k.ValueType)
	return h.Sum64()
}

func (k numberDataPointKey) Equal(other numberDataPointKey) bool {
	return k.StartTimestamp == other.StartTimestamp &&
		k.Attributes.Equal(other.Attributes) &&
		k.ValueType == other.ValueType
}

type histogramDataPointKey struct {
	StartTimestamp pcommon.Timestamp
	Attributes     pcommon.Map
	ExplicitBounds pcommon.Float64Slice
}

func newHistogramDataPointKey(dp pmetric.HistogramDataPoint) histogramDataPointKey {
	return histogramDataPointKey{
		StartTimestamp: dp.StartTimestamp(),
		Attributes:     dp.Attributes(),
		ExplicitBounds: dp.ExplicitBounds(),
	}
}

func (k histogramDataPointKey) Hash() uint64 {
	var h maphash.Hash
	h.SetSeed(maphashSeed)

	maphash.WriteComparable(&h, k.StartTimestamp)
	maphash.WriteComparable(&h, pdatautil.MapHash(k.Attributes))

	maphash.WriteComparable(&h, k.ExplicitBounds.Len())
	for _, bound := range k.ExplicitBounds.All() {
		maphash.WriteComparable(&h, bound)
	}
	return h.Sum64()
}

func (k histogramDataPointKey) Equal(other histogramDataPointKey) bool {
	return k.StartTimestamp == other.StartTimestamp &&
		k.Attributes.Equal(other.Attributes) &&
		k.ExplicitBounds.Equal(other.ExplicitBounds)
}

type exponentialHistogramDataPointKey struct {
	StartTimestamp pcommon.Timestamp
	Attributes     pcommon.Map
	Scale          int32
	ZeroThreshold  float64
}

func newExponentialHistogramDataPointKey(
	dp pmetric.ExponentialHistogramDataPoint,
) exponentialHistogramDataPointKey {
	return exponentialHistogramDataPointKey{
		StartTimestamp: dp.StartTimestamp(),
		Attributes:     dp.Attributes(),
		Scale:          dp.Scale(),
		ZeroThreshold:  dp.ZeroThreshold(),
	}
}

func (k exponentialHistogramDataPointKey) Hash() uint64 {
	var h maphash.Hash
	h.SetSeed(maphashSeed)

	maphash.WriteComparable(&h, k.StartTimestamp)
	maphash.WriteComparable(&h, pdatautil.MapHash(k.Attributes))
	maphash.WriteComparable(&h, k.Scale)
	maphash.WriteComparable(&h, k.ZeroThreshold)

	return h.Sum64()
}

func (k exponentialHistogramDataPointKey) Equal(other exponentialHistogramDataPointKey) bool {
	return k.StartTimestamp == other.StartTimestamp &&
		k.Attributes.Equal(other.Attributes) &&
		k.Scale == other.Scale &&
		k.ZeroThreshold == other.ZeroThreshold
}
