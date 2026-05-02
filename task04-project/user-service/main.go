// user-service. Принимает запросы /user/{id}, "читает" из БД, отвечает.
//
// Порт :8083, эндпоинты:
//   GET /user/{id}  - вернёт "user-<id>"
//   GET /metrics    - метрики для Prometheus
//
// Этот сервис вызывается из order-service (task04-project/order-service).
// В Jaeger оба должны оказаться в одном trace - за это отвечает otelhttp.NewHandler,
// который прочитает traceparent-заголовок из входящего запроса.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "user-service"

// tracer получаем один раз. Имя попадает в поле "instrumentation.name" в Jaeger -
// помогает различать спаны от разных пакетов/библиотек.
var tracer = otel.Tracer("task04/user-service")

// callDB - имитация похода в БД. Всё интересное тут - в том, что у неё свой спан:
// в Jaeger ты увидишь отдельный узел в дереве именно под "callDB".
func callDB(ctx context.Context, id string) (string, error) {
	// Параметр ctx - ВАЖЕН. Если тут взять context.Background(), спан callDB
	// окажется корневым, никак не связанным с HTTP-запросом, и в Jaeger
	// ты увидишь "сиротский" trace без родителя.
	_, span := tracer.Start(ctx, "callDB",
		trace.WithAttributes(
			// Стандартные ключи semconv - их понимают и Jaeger, и Datadog, и все остальные.
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "SELECT * FROM users WHERE id=$1"),
			attribute.String("user.id", id),
		),
	)
	defer span.End()

	// 20..100ms - реалистичная задержка локального Postgres.
	time.Sleep(time.Duration(20+rand.Intn(80)) * time.Millisecond)

	if id == "" {
		err := errors.New("empty user id")
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}
	return "user-" + id, nil
}

func userHandler(w http.ResponseWriter, r *http.Request) {
	// r.Context() содержит корневой спан, созданный otelhttp-middleware (см. main).
	// Передаём его в callDB - и callDB автоматически становится child-ом.
	ctx := r.Context()
	id := strings.TrimPrefix(r.URL.Path, "/user/")

	user, err := callDB(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = fmt.Fprintln(w, user)
}

func main() {
	ctx := context.Background()

	// Поднимаем TracerProvider (код в obs.go).
	shutdown, err := initTracer(ctx, serviceName)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer func() {
		// Отдельный ctx с таймаутом - чтобы если Jaeger уже лёг, процесс
		// не висел навечно в попытке слить буфер.
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(c)
	}()

	mux := http.NewServeMux()

	// TODO 1: зарегистрируй userHandler с двумя middleware - metricsMiddleware
	// и otelhttp.NewHandler. Порядок важен.
	//
	//   userH := otelhttp.NewHandler(
	//       metricsMiddleware(http.HandlerFunc(userHandler)),
	//       "GET /user/:id",
	//   )
	//   mux.Handle("/user/", userH)
	//
	// Почему otelhttp - внешний:
	//   otelhttp смотрит на входящий запрос РАНЬШЕ всех. Он читает traceparent
	//   из заголовков и создаёт корневой спан, который потом через r.Context()
	//   доступен внутри хендлера и всех его child-спанов.
	//
	// Почему второй аргумент - "GET /user/:id", а не просто "user":
	//   Это имя дефолтного корневого спана. В Jaeger оно будет отображаться
	//   как operation name. Удобно, если все твои трейсы называются одинаково
	//   осмысленно, а не "http.server".
	_ = userHandler
	_ = otelhttp.NewHandler
	_ = metricsMiddleware

	// /metrics мы НЕ оборачиваем в otelhttp/metricsMiddleware:
	//   - оборачивать в metricsMiddleware - самопересечение (каждый скрейп
	//     стал бы считаться "запросом к сервису" и раздувал бы счётчик).
	//   - оборачивать в otelhttp - просто мусор в Jaeger, Prometheus долбится
	//     раз в 5 секунд и нам не нужны trace-ы на каждый скрейп.
	mux.Handle("/metrics", promhttp.Handler())

	addr := ":8083"
	log.Printf("%s listening on %s", serviceName, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
