// order-service. Принимает /order/{id}, идёт в user-service за пользователем,
// отвечает "order for user-<id>".
//
// Порт :8082, эндпоинты:
//   GET /order/{id} - бизнес-логика
//   GET /metrics    - метрики для Prometheus
//
// САМАЯ ВАЖНАЯ СТРОЧКА ВО ВСЁМ ПРОЕКТЕ:
//   Transport: otelhttp.NewTransport(http.DefaultTransport)
// Именно она делает так, что при походе в user-service к запросу
// прикрепляется traceparent-заголовок и trace продолжается в user-service.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	serviceName    = "order-service"
	userServiceURL = "http://localhost:8083"
)

var tracer = otel.Tracer("task04/order-service")

// httpClient - тот самый клиент, через который order-service ходит в user-service.
//
// otelhttp.NewTransport оборачивает стандартный http.RoundTripper. На каждый исходящий
// запрос транспорт делает:
//  1. создаёт "клиентский" спан с именем HTTP-метода (GET/POST),
//  2. достаёт из ctx активный родительский спан и делает новый его child-ом,
//  3. сериализует trace-context в заголовок traceparent (через пропагатор,
//     настроенный в initTracer).
//
// Если бы мы писали просто:
//
//	httpClient := &http.Client{}
//
// то получили бы:
//   - trace обрывается в момент вызова (в Jaeger два куска)
//   - нет клиентского спана "GET http://...", а это очень полезный узел в дереве.
//
// Timeout 2s - на учебном стенде этого с запасом. В проде обычно держат строже
// и настраивают более гранулярно (dial, TLS, response header).
var httpClient = &http.Client{
	Transport: otelhttp.NewTransport(http.DefaultTransport),
	Timeout:   2 * time.Second,
}

// fetchUser - поход в user-service. Весь "магический" кусок трейсинга - внутри.
func fetchUser(ctx context.Context, id string) (string, error) {
	ctx, span := tracer.Start(ctx, "fetchUser")
	defer span.End()
	// attributes помогают потом в UI фильтровать и искать ("все trace-ы где user.id=42").
	span.SetAttributes(attribute.String("user.id", id))

	// ВАЖНО: http.NewRequestWithContext, а не http.NewRequest.
	// otelhttp.Transport достаёт активный спан именно из ctx запроса. Если ctx
	// не передан - трансфер создаст "сиротский" клиентский спан, не связанный
	// с текущим trace-ом.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		userServiceURL+"/user/"+id, nil)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}

	// Вот тут происходит чудо: инструментированный Transport добавляет traceparent
	// и отправляет запрос.
	resp, err := httpClient.Do(req)
	if err != nil {
		// Сетевая ошибка (user-service не запущен/упал/таймаут). Пометим спан.
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Ответ от user-service пришёл, но код не 200. Тоже ошибка с нашей точки зрения.
		err := fmt.Errorf("user-service: %s", resp.Status)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

// validateOrder - простая валидация id. Создаёт собственный спан чтобы было видно
// в Jaeger, сколько времени ушло именно на валидацию (обычно ≤1ms).
func validateOrder(ctx context.Context, id string) error {
	_, span := tracer.Start(ctx, "validateOrder")
	defer span.End()
	span.SetAttributes(attribute.String("order.id", id))

	if strings.TrimSpace(id) == "" {
		err := errors.New("empty order id")
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}
	return nil
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	// r.Context() содержит корневой спан от otelhttp.NewHandler (см. main).
	// Все дочерние спаны ниже будут висеть под ним.
	ctx := r.Context()
	id := strings.TrimPrefix(r.URL.Path, "/order/")

	if err := validateOrder(ctx, id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	user, err := fetchUser(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = fmt.Fprintf(w, "order for %s (item=book)\n", user)
}

func main() {
	ctx := context.Background()

	shutdown, err := initTracer(ctx, serviceName)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(c)
	}()

	mux := http.NewServeMux()

	// TODO 1: зарегистрируй orderHandler с двумя middleware. То же самое, что
	// в user-service:
	//
	//   orderH := otelhttp.NewHandler(
	//       metricsMiddleware(http.HandlerFunc(orderHandler)),
	//       "GET /order/:id",
	//   )
	//   mux.Handle("/order/", orderH)
	//
	// Зачем otelhttp.NewHandler на входе, если мы "клиент" относительно user-service?
	//   Мы одновременно и клиент (идём в user-service), и сервер (к нам приходят
	//   от curl/hey). otelhttp.NewHandler отвечает за СЕРВЕРНУЮ часть -
	//   создаёт корневой спан для ВХОДЯЩИХ запросов. А otelhttp.NewTransport -
	//   за клиентскую, для ИСХОДЯЩИХ. Нужны оба.
	_ = orderHandler
	_ = metricsMiddleware
	_ = otelhttp.NewHandler

	mux.Handle("/metrics", promhttp.Handler())

	addr := ":8082"
	log.Printf("%s listening on %s", serviceName, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
