package downsamplingprocessor

import (
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
)

var signalMetrics = attribute.String("otel.signal", pipeline.SignalMetrics.String())
