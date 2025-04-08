package aggregate

import (
	"iter"
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type (
	Aggregator struct {
		logger                        *zap.Logger
		cardinality                   uint32
		cardinalityLimit              uint32
		cardinalityExceededItemsCount int64
		resources                     Map[resourceKey, resourceAggregator]
	}
	resourceAggregator struct {
		scopes Map[scopeKey, scopeAggregator]
	}
	scopeAggregator struct {
		metrics Map[metricKey, metricAggregator]
	}
	metricAggregator struct {
		numberDataPoints               Map[numberDataPointKey, pmetric.NumberDataPoint]
		histogramDataPoints            Map[histogramDataPointKey, pmetric.HistogramDataPoint]
		exponentialHistogramDataPoints Map[exponentialHistogramDataPointKey, pmetric.ExponentialHistogramDataPoint]
	}
)

func NewAggregator(logger *zap.Logger, cardinalityLimit uint32) *Aggregator {
	return &Aggregator{
		logger:                        logger,
		cardinality:                   0,
		cardinalityLimit:              cardinalityLimit,
		cardinalityExceededItemsCount: 0,
		resources:                     NewMap[resourceKey, resourceAggregator](),
	}
}

func newResourceAggregator() resourceAggregator {
	return resourceAggregator{
		scopes: NewMap[scopeKey, scopeAggregator](),
	}
}

func newScopeAggregator() scopeAggregator {
	return scopeAggregator{
		metrics: NewMap[metricKey, metricAggregator](),
	}
}

func newMetricAggregator() metricAggregator {
	return metricAggregator{
		numberDataPoints:               NewMap[numberDataPointKey, pmetric.NumberDataPoint](),
		histogramDataPoints:            NewMap[histogramDataPointKey, pmetric.HistogramDataPoint](),
		exponentialHistogramDataPoints: NewMap[exponentialHistogramDataPointKey, pmetric.ExponentialHistogramDataPoint](),
	}
}

func (a *Aggregator) Reset() {
	a.cardinality = 0
	a.resources = NewMap[resourceKey, resourceAggregator]()
}

func (a *Aggregator) Cardinality() uint32 {
	return a.cardinality
}

func (a *Aggregator) CardinalityLimit() uint32 {
	return a.cardinalityLimit
}

func (a *Aggregator) CardinalityExceededItemsCount() int64 {
	return a.cardinalityExceededItemsCount
}

func (a *Aggregator) ExportMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()

	rms := md.ResourceMetrics()
	for rkey, ragg := range a.resources.All() {
		rm := rms.AppendEmpty()
		pcommon.Resource(rkey).CopyTo(rm.Resource())

		sms := rm.ScopeMetrics()
		for skey, sagg := range ragg.scopes.All() {
			sm := sms.AppendEmpty()
			pcommon.InstrumentationScope(skey).CopyTo(sm.Scope())

			ms := sm.Metrics()
			for mkey, magg := range sagg.metrics.All() {
				m := ms.AppendEmpty()
				m.SetName(mkey.Name)
				m.SetUnit(mkey.Unit)

				switch mkey.Type {
				case pmetric.MetricTypeGauge:
					gauge := m.SetEmptyGauge()
					exportDataPoints(gauge.DataPoints(), magg.numberDataPoints.Values())
				case pmetric.MetricTypeSum:
					sum := m.SetEmptySum()
					sum.SetAggregationTemporality(mkey.AggregationTemporality)
					sum.SetIsMonotonic(mkey.IsMonotonic)
					exportDataPoints(sum.DataPoints(), magg.numberDataPoints.Values())
				case pmetric.MetricTypeHistogram:
					histogram := m.SetEmptyHistogram()
					histogram.SetAggregationTemporality(mkey.AggregationTemporality)
					exportDataPoints(histogram.DataPoints(), magg.histogramDataPoints.Values())
				case pmetric.MetricTypeExponentialHistogram:
					expHistogram := m.SetEmptyExponentialHistogram()
					expHistogram.SetAggregationTemporality(mkey.AggregationTemporality)
					exportDataPoints(
						expHistogram.DataPoints(),
						magg.exponentialHistogramDataPoints.Values(),
					)
				}
			}
		}
	}

	return md
}

type dataPoint[DP any] interface {
	Timestamp() pcommon.Timestamp
	CopyTo(DP)
}

type dataPointSlice[DP dataPoint[DP]] interface {
	AppendEmpty() DP
}

func exportDataPoints[DP dataPoint[DP], S dataPointSlice[DP]](
	dst S, src iter.Seq[DP],
) {
	dps := slices.Collect(src)
	slices.SortFunc(dps, func(a, b DP) int {
		return a.Timestamp().AsTime().Compare(b.Timestamp().AsTime())
	})
	for _, dp := range dps {
		dp.CopyTo(dst.AppendEmpty())
	}
}

func (a *Aggregator) AddMetrics(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		resource := rm.Resource()
		rkey := resourceKey(resource)
		ragg, ok := a.resources.Get(rkey)
		if !ok {
			ragg = newResourceAggregator()
			a.resources.Set(rkey, ragg)
		}

		sms := rm.ScopeMetrics()
		sms.RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			scope := sm.Scope()
			skey := scopeKey(scope)
			sagg, ok := ragg.scopes.Get(skey)
			if !ok {
				sagg = newScopeAggregator()
				ragg.scopes.Set(skey, sagg)
			}

			ms := sm.Metrics()
			ms.RemoveIf(func(m pmetric.Metric) bool {
				mkey := newMetricKey(m)
				magg, ok := sagg.metrics.Get(mkey)
				if !ok {
					magg = newMetricAggregator()
					sagg.metrics.Set(mkey, magg)
				}

				if !aggregationTemporalitySupported(mkey.AggregationTemporality) {
					a.logger.Debug(
						"Unsupported aggregation temporality",
						zap.String("aggregation_temporality", mkey.AggregationTemporality.String()),
					)
					return false
				}

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					gauge := m.Gauge()
					gauge.DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						return a.addNumberDataPoint(magg, mkey.AggregationTemporality, dp)
					})
					return gauge.DataPoints().Len() == 0
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					sum.SetAggregationTemporality(mkey.AggregationTemporality)
					sum.SetIsMonotonic(mkey.IsMonotonic)
					sum.DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						return a.addNumberDataPoint(magg, mkey.AggregationTemporality, dp)
					})
					return sum.DataPoints().Len() == 0
				case pmetric.MetricTypeHistogram:
					hist := m.Histogram()
					hist.SetAggregationTemporality(mkey.AggregationTemporality)
					hist.DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						return a.addHistogramDataPoint(magg, mkey.AggregationTemporality, dp)
					})
					return hist.DataPoints().Len() == 0
				case pmetric.MetricTypeExponentialHistogram:
					expHist := m.ExponentialHistogram()
					expHist.SetAggregationTemporality(mkey.AggregationTemporality)
					expHist.DataPoints().
						RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
							return a.addExponentialHistogramDataPoint(
								magg,
								mkey.AggregationTemporality,
								dp,
							)
						})
					return expHist.DataPoints().Len() == 0
				case pmetric.MetricTypeSummary:
					// summary is not supported
					return false
				default:
					a.logger.Debug("Unsupported metric type", zap.String("type", m.Type().String()))
					return false
				}
			})
			return ms.Len() == 0
		})
		return sms.Len() == 0
	})
}

func (a *Aggregator) addNumberDataPoint(
	magg metricAggregator,
	aggregationTemporality pmetric.AggregationTemporality,
	dp pmetric.NumberDataPoint,
) bool {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt, pmetric.NumberDataPointValueTypeDouble:
	default:
		a.logger.Debug(
			"Unsupported number data point value type",
			zap.String("value_type", dp.ValueType().String()),
		)
		return false
	}

	key := newNumberDataPointKey(dp)
	currentDP, ok := magg.numberDataPoints.Get(key)
	if ok {
		aggregateNumberDataPoint(currentDP, dp, aggregationTemporality)
		return true
	} else {
		if ok := a.incrementMetricCardinality(); !ok {
			a.cardinalityExceededItemsCount++
			return false
		}
		newDP := pmetric.NewNumberDataPoint()
		dp.CopyTo(newDP)
		magg.numberDataPoints.Set(key, newDP)
		return true
	}
}

func (a *Aggregator) addHistogramDataPoint(
	magg metricAggregator,
	aggregationTemporality pmetric.AggregationTemporality,
	dp pmetric.HistogramDataPoint,
) bool {
	key := newHistogramDataPointKey(dp)
	currentDP, ok := magg.histogramDataPoints.Get(key)
	if ok {
		aggregateHistogramDataPoint(currentDP, dp, aggregationTemporality)
		return true
	} else {
		if ok := a.incrementMetricCardinality(); !ok {
			a.cardinalityExceededItemsCount++
			return false
		}
		newDP := pmetric.NewHistogramDataPoint()
		dp.CopyTo(newDP)
		magg.histogramDataPoints.Set(key, newDP)
		return true
	}
}

func (a *Aggregator) addExponentialHistogramDataPoint(
	magg metricAggregator,
	aggregationTemporality pmetric.AggregationTemporality,
	dp pmetric.ExponentialHistogramDataPoint,
) bool {
	key := newExponentialHistogramDataPointKey(dp)
	currentDP, ok := magg.exponentialHistogramDataPoints.Get(key)
	if ok {
		aggregateExponentialHistogramDataPoint(currentDP, dp, aggregationTemporality)
		return true
	} else {
		if ok := a.incrementMetricCardinality(); !ok {
			a.cardinalityExceededItemsCount++
			return false
		}
		newDP := pmetric.NewExponentialHistogramDataPoint()
		dp.CopyTo(newDP)
		magg.exponentialHistogramDataPoints.Set(key, newDP)
		return true
	}
}

func aggregationTemporalitySupported(aggregationTemporality pmetric.AggregationTemporality) bool {
	switch aggregationTemporality {
	case pmetric.AggregationTemporalityUnspecified,
		pmetric.AggregationTemporalityDelta,
		pmetric.AggregationTemporalityCumulative:
		return true
	default:
		return false
	}
}

func aggregateNumberDataPoint(
	dst, dp pmetric.NumberDataPoint,
	aggregationTemporality pmetric.AggregationTemporality,
) {
	if dp.Flags().NoRecordedValue() {
		return
	}

	aggregateExemplars(dst.Exemplars(), dp.Exemplars())
	dst.SetTimestamp(dp.Timestamp())
	dst.SetFlags(dp.Flags())

	switch dst.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		if aggregationTemporality != pmetric.AggregationTemporalityDelta {
			dst.SetIntValue(dp.IntValue())
		} else /* pmetric.AggregationTemporalityDelta */ {
			dst.SetIntValue(dp.IntValue() + dst.IntValue())
		}
	case pmetric.NumberDataPointValueTypeDouble:
		if aggregationTemporality != pmetric.AggregationTemporalityDelta {
			dst.SetDoubleValue(dp.DoubleValue())
		} else /* pmetric.AggregationTemporalityDelta */ {
			dst.SetDoubleValue(dp.DoubleValue() + dst.DoubleValue())
		}
	}
}

func aggregateHistogramDataPoint(
	dst, dp pmetric.HistogramDataPoint,
	aggregationTemporality pmetric.AggregationTemporality,
) {
	if dp.Flags().NoRecordedValue() {
		return
	}

	aggregateExemplars(dst.Exemplars(), dp.Exemplars())
	dst.SetTimestamp(dp.Timestamp())
	dst.SetFlags(dp.Flags())

	if aggregationTemporality != pmetric.AggregationTemporalityDelta {
		dp.BucketCounts().CopyTo(dst.BucketCounts())

		if dp.HasSum() {
			dst.SetSum(dp.Sum())
		}
		if dp.HasMin() {
			dst.SetMin(dp.Min())
		}
		if dp.HasMax() {
			dst.SetMax(dp.Max())
		}
	} else /* pmetric.AggregationTemporalityDelta */ {
		dstBuckets, buckets := dst.BucketCounts(), dp.BucketCounts()
		for i := range dstBuckets.Len() {
			dstBuckets.SetAt(i, dstBuckets.At(i)+buckets.At(i))
		}

		if dst.HasSum() && dp.HasSum() {
			dst.SetSum(dst.Sum() + dp.Sum())
		}
		if dst.HasMin() && dp.HasMin() {
			dst.SetMin(min(dst.Min(), dp.Min()))
		}
		if dst.HasMax() && dp.HasMax() {
			dst.SetMax(max(dst.Max(), dp.Max()))
		}
	}
}

func aggregateExponentialHistogramDataPoint(
	dst, dp pmetric.ExponentialHistogramDataPoint,
	aggregationTemporality pmetric.AggregationTemporality,
) {
	if dp.Flags().NoRecordedValue() {
		return
	}

	aggregateExemplars(dst.Exemplars(), dp.Exemplars())
	dst.SetTimestamp(dp.Timestamp())
	dst.SetFlags(dp.Flags())

	if aggregationTemporality != pmetric.AggregationTemporalityDelta {
		dst.SetZeroCount(dp.ZeroCount())
		dp.Positive().CopyTo(dst.Positive())
		dp.Negative().CopyTo(dst.Negative())

		if dp.HasSum() {
			dst.SetSum(dp.Sum())
		}
		if dp.HasMin() {
			dst.SetMin(dp.Min())
		}
		if dp.HasMax() {
			dst.SetMax(dp.Max())
		}
	} else /* pmetric.AggregationTemporalityDelta */ {
		dst.SetZeroCount(dst.ZeroCount() + dp.ZeroCount())
		addExponentialHistogramBuckets(dst.Positive(), dp.Positive())
		addExponentialHistogramBuckets(dst.Negative(), dp.Negative())

		if dst.HasSum() && dp.HasSum() {
			dst.SetSum(dst.Sum() + dp.Sum())
		}
		if dst.HasMin() && dp.HasMin() {
			dst.SetMin(min(dst.Min(), dp.Min()))
		}
		if dst.HasMax() && dp.HasMax() {
			dst.SetMax(max(dst.Max(), dp.Max()))
		}
	}
}

func aggregateExemplars(dst, dp pmetric.ExemplarSlice) {
	for i := range dp.Len() {
		dp.At(i).CopyTo(dst.AppendEmpty())
	}
}

func addExponentialHistogramBuckets(dst, dp pmetric.ExponentialHistogramDataPointBuckets) {
	dstBuckets, dpBuckets := dst.BucketCounts(), dp.BucketCounts()
	lo1, lo2 := int(dst.Offset()), int(dp.Offset())
	hi1, hi2 := lo1+dstBuckets.Len(), lo2+dpBuckets.Len()

	lo, hi := min(lo1, lo2), max(hi1, hi2)
	dst.SetOffset(int32(lo))
	bucketsLen := hi - lo // >= max(dstBuckets.Len(), dpBuckets.Len())
	dstBuckets.EnsureCapacity(bucketsLen)
	for dstBuckets.Len() < bucketsLen {
		dstBuckets.Append(0)
	}
	for i := range bucketsLen {
		idx := i + lo
		var bucket uint64
		if lo1 <= idx && idx < hi1 {
			bucket += dstBuckets.At(idx - lo1)
		}
		if lo2 <= idx && idx < hi2 {
			bucket += dpBuckets.At(idx - lo2)
		}
		dstBuckets.SetAt(i, bucket)
	}
}

func (a *Aggregator) incrementMetricCardinality() bool {
	if a.cardinality >= a.cardinalityLimit {
		return false
	}
	a.cardinality++
	return true
}
