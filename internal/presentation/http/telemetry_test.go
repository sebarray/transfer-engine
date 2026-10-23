package httpapi

import (
	"bytes"
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"transfer-engine/internal/business/health"
	"transfer-engine/internal/business/transfers"
)

func TestTelemetryCorrelationAndBoundedLabels(t *testing.T) {
	previousTrace, previousMeter, previousProp := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(previousTrace)
		otel.SetMeterProvider(previousMeter)
		otel.SetTextMapPropagator(previousProp)
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	}()
	var logs bytes.Buffer
	e := New(health.New(healthy{}, time.Second), slog.New(slog.NewJSONHandler(&logs, nil)), 1024)
	store := &financialStore{}
	service, _ := transfers.New(store, "test", false)
	RegisterFinancial(e, service, transfers.NewQueries(store), testAuth{}, "test", false)
	id := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	r := httptest.NewRequest("GET", "/v1/accounts/"+id+"?secret=PRIVATE", nil)
	r.Header.Set("Authorization", "Bearer SECRET_TOKEN")
	r.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].SpanContext().TraceID().String() != "11111111111111111111111111111111" || spans[0].Parent().SpanID().String() != "2222222222222222" {
		t.Fatal("trace propagation failed")
	}
	for _, forbidden := range []string{id, "PRIVATE", "SECRET_TOKEN"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Fatal("sensitive log data")
		}
	}
	if !strings.Contains(logs.String(), `"trace_id":"11111111111111111111111111111111"`) || !strings.Contains(logs.String(), w.Header().Get("X-Request-ID")) {
		t.Fatal("log correlation missing")
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					found = true
					for _, a := range point.Attributes.ToSlice() {
						switch string(a.Key) {
						case "http.request.method", "http.route", "http.response.status_code":
						default:
							t.Fatalf("unbounded attribute %s", a.Key)
						}
						if strings.Contains(a.Value.Emit(), id) {
							t.Fatal("identifier in metric")
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("request metric missing")
	}
}
