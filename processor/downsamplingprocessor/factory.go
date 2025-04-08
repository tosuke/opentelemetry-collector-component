package downsamplingprocessor

import (
	"context"
	"fmt"

	"github.com/tosuke/opentelemetry-collector-components/processor/downsamplingprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
	)
}

func createMetricsProcessor(
	_ context.Context,
	settings processor.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (processor.Metrics, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	if err := processorCfg.Validate(); err != nil {
		return nil, err
	}

	downsamplingProcessor, err := newDownsamplingProcessor(settings, processorCfg, consumer)
	if err != nil {
		return nil, err
	}

	return downsamplingProcessor, nil
}
