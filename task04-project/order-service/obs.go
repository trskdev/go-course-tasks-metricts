// obs.go - ГОТОВАЯ обвязка observability.
//
// Мы нарочно дописали этот файл за тебя. В task01/task03 ты уже проходил каждую
// строчку здесь вручную - повторяться нет смысла. Тут всё то же самое, собранное
// в одном месте, чтобы в main.go ты сосредоточился на архитектурной части задачи:
// как связать два сервиса в один trace.
//
// Если захочешь разобрать этот файл построчно - смотри комментарии в task01
// (метрики) и task03 (трейсинг).

package main

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// --- Метрики RED ----------------------------------------------------
//
// Одинаковые имена метрик в двух сервисах - это ПРАВИЛЬНО. Имя + лейблы
// образуют идентификатор. Prometheus по scrape-конфигу добавляет к ним
// лейбл service={order|user} (см. infra/prometheus.yml). В итоге в БД
// это два разных ряда, но в PromQL их удобно джойнить одной формулой.

var (
	httpRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP-запросы: счётчик по методу/пути/статусу.",
		},
		[]string{"method", "path", "status"},
	)

	httpDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Длительность обработки HTTP-запроса, в секундах.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)
)

// statusRecorder перехватывает код ответа, чтобы middleware мог положить его
// в лейбл метрики. Подробнее - в task01/main.go.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// metricsMiddleware считает RED-метрики для любого обёрнутого хендлера.
// На каждый запрос: +1 в counter, длительность в histogram.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		status := strconv.Itoa(rec.status)
		dur := time.Since(start).Seconds()
		httpRequests.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		httpDuration.WithLabelValues(r.Method, r.URL.Path, status).Observe(dur)
	})
}

// --- Трейсинг --------------------------------------------------------
//
// initTracer настраивает глобальный TracerProvider: OTLP-HTTP в Jaeger,
// сэмплирование 100%, пропагация по W3C TraceContext + Baggage.
// Возвращает shutdown-функцию - обязательно вызови её defer в main.
//
// Ключевая штука для task04: propagation.TraceContext{} в SetTextMapPropagator.
// Именно он отвечает за то, чтобы:
//   - при исходящем запросе (через otelhttp.NewTransport) добавлялся заголовок
//     traceparent,
//   - при входящем (через otelhttp.NewHandler) заголовок читался и trace
//     продолжался, а не начинался заново.
// Если убрать - два сервиса окажутся в разных trace-ах.
func initTracer(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	// Имя сервиса - единственный обязательный атрибут. По нему Jaeger и Grafana
	// идентифицируют сервис в UI.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.DeploymentEnvironment("local"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
