package downsamplingprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tosuke/opentelemetry-collector-components/processor/downsamplingprocessor/internal/aggregate"
	"github.com/tosuke/opentelemetry-collector-components/processor/downsamplingprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type downsamplingProcessor struct {
	logger *zap.Logger

	mu           sync.Mutex
	emitInterval time.Duration
	aggregator   *aggregate.Aggregator
	nextConsumer consumer.Metrics

	shutdown func()
	wg       *sync.WaitGroup

	telemetry *metadata.TelemetryBuilder
}

func newDownsamplingProcessor(
	settings processor.Settings,
	cfg *Config,
	nextConsumer consumer.Metrics,
) (*downsamplingProcessor, error) {
	logger := settings.Logger

	telemetry, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to init telemetry: %w", err)
	}

	aggregator := aggregate.NewAggregator(logger, cfg.CardinalityLimit)

	return &downsamplingProcessor{
		logger:       logger,
		emitInterval: cfg.Interval,
		aggregator:   aggregator,
		nextConsumer: nextConsumer,
		wg:           &sync.WaitGroup{},
		telemetry:    telemetry,
	}, nil
}

func (p *downsamplingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *downsamplingProcessor) Start(ctx context.Context, _ component.Host) error {
	if err := p.telemetry.RegisterProcessorDownsamplingCardinalityCallback(func(_ context.Context, io metric.Int64Observer) error {
		p.mu.Lock()
		defer p.mu.Unlock()

		io.Observe(int64(p.aggregator.Cardinality()))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetry.RegisterProcessorDownsamplingCardinalityLimitCallback(func(_ context.Context, io metric.Int64Observer) error {
		p.mu.Lock()
		defer p.mu.Unlock()

		io.Observe(int64(p.aggregator.CardinalityLimit()))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetry.RegisterProcessorDownsamplingCardinalityExceededItemsCallback(func(_ context.Context, io metric.Int64Observer) error {
		p.mu.Lock()
		defer p.mu.Unlock()

		io.Observe(p.aggregator.CardinalityExceededItemsCount())
		return nil
	}); err != nil {
		return err
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	p.shutdown = cancel
	p.wg.Add(1)
	go p.exportLoop(loopCtx)

	return nil
}

func (p *downsamplingProcessor) Shutdown(ctx context.Context) error {
	defer p.telemetry.Shutdown()

	if p.shutdown != nil {
		p.shutdown()
		p.wg.Wait()
	}
	if err := p.exportMetrics(ctx, p.exportMetricsData()); err != nil {
		return err
	}
	return nil
}

func (p *downsamplingProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.telemetry.ProcessorIncomingItems.Add(
		ctx,
		int64(md.DataPointCount()),
		metric.WithAttributes(signalMetrics),
	)

	p.aggregateMetrics(md)
	if md.DataPointCount() > 0 {
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	}
	return nil
}

func (p *downsamplingProcessor) exportLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.emitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.logger.Debug("Exporting metrics")
			if err := p.exportMetricsInLoop(ctx); err != nil {
				p.logger.Error("Failed to export metrics", zap.Error(err))
			}
		}
	}
}

func (p *downsamplingProcessor) exportMetricsInLoop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.emitInterval)
	defer cancel()
	return p.exportMetrics(ctx, p.exportMetricsData())
}

func (p *downsamplingProcessor) exportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if count := md.DataPointCount(); count > 0 {
		p.telemetry.ProcessorOutgoingItems.Add(
			ctx,
			int64(count),
			metric.WithAttributes(signalMetrics),
		)
		if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
			return err
		}
	}

	return nil
}

func (p *downsamplingProcessor) exportMetricsData() pmetric.Metrics {
	p.mu.Lock()
	defer p.mu.Unlock()

	md := p.aggregator.ExportMetrics()
	p.aggregator.Reset()
	return md
}

func (p *downsamplingProcessor) aggregateMetrics(md pmetric.Metrics) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.aggregator.AddMetrics(md)
}
