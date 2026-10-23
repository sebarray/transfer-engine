// Package telemetry owns SDK lifecycle; business remains independent of OTel.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func Start(ctx context.Context, endpoint string, logger *slog.Logger) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{}) // Deliberately no baggage/PII propagation.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) { logger.Warn("telemetry_export_failed") }))
	res := resource.NewSchemaless(attribute.String("service.name", "transfer-engine"))
	var opts []sdktrace.TracerProviderOption
	var metrics []sdkmetric.Option
	opts = append(opts, sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	metrics = append(metrics, sdkmetric.WithResource(res))
	if endpoint != "" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithTimeout(time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if err != nil {
			return nil, errors.New("trace exporter configuration failed")
		}
		mexp, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint), otlpmetrichttp.WithTimeout(time.Second), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
		if err != nil {
			_ = exp.Shutdown(ctx)
			return nil, errors.New("metric exporter configuration failed")
		}
		opts = append(opts, sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(512), sdktrace.WithMaxExportBatchSize(128), sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(time.Second)))
		metrics = append(metrics, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(10*time.Second), sdkmetric.WithTimeout(time.Second))))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	mp := sdkmetric.NewMeterProvider(metrics...)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	return func(ctx context.Context) error { return errors.Join(mp.Shutdown(ctx), tp.Shutdown(ctx)) }, nil
}
