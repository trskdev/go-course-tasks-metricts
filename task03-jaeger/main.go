// task03: инструментируем HTTP-сервис трейсами OpenTelemetry и шлём их в Jaeger.
//
// Сервис на :8082, эндпоинт /order/{id}. На каждый запрос создаётся trace:
//
//   HTTP /order              ← корневой спан, создаёт otelhttp-middleware
//     ├── validateInput      ← дочерний, создаём сами в коде
//     └── callDB             ← дочерний, имитирует запрос в БД
//
// Запуск:
//   go mod tidy
//   go run main.go

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

	// otelhttp - готовое middleware для net/http. На каждый входящий запрос:
	//  1) создаёт корневой спан,
	//  2) читает заголовок traceparent (если кто-то вызвал наш сервис с trace-ом,
	//     мы продолжим его trace, а не начнём новый),
	//  3) прокидывает контекст со спаном в r.Context().
	// Без него пришлось бы всё это делать руками в каждом хендлере.
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	// API - то, чем пользуется бизнес-код. Никакой настройки, никаких экспортеров.
	// Бизнес-код никогда не знает, куда спаны в итоге уходят.
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	// Exporter. Бывают разные (stdout, Zipkin, gRPC/HTTP OTLP). Мы берём OTLP-HTTP:
	// спаны шлются по HTTP на коллектор (у нас Jaeger на :4318).
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	// SDK - "мясо" трейсинга. Провайдер, сэмплер, батчер. Настраиваем один раз в main.
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	// semconv - пакет с "каноническими" ключами атрибутов. Вместо "service.name"
	// строкой используем semconv.ServiceName - при обновлении спецификации имя
	// атрибута обновится автоматически.
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Имя сервиса - как он будет виден в Jaeger UI.
// Его же использует resource (см. ниже) - поэтому вынесли в константу.
const serviceName = "task03-jaeger"

// =====================================================================
// Инициализация трейсинга
// =====================================================================

// initTracer - единственное место, где собираем и регистрируем TracerProvider.
// Возвращает shutdown-функцию - её обязательно вызвать при завершении процесса,
// иначе BatchSpanProcessor не успеет отправить буфер в Jaeger и последние
// несколько десятков спанов просто потеряются.
func initTracer(ctx context.Context) (func(context.Context) error, error) {
	// TODO 1: создай OTLP-HTTP экспортер.
	//
	//   exporter, err := otlptracehttp.New(ctx,
	//       otlptracehttp.WithEndpoint("localhost:4318"),  // хост:порт, БЕЗ схемы
	//       otlptracehttp.WithInsecure(),                   // без TLS, для локалки ок
	//   )
	//   if err != nil {
	//       return nil, err
	//   }
	//
	// Зачем WithInsecure: по умолчанию OTLP-клиент пытается в HTTPS, и без сертификата
	// у него ничего не выйдет. Для локального Jaeger (в docker-compose) TLS лишний.
	// В проде этого флага быть не должно.
	var exporter sdktrace.SpanExporter
	_ = exporter
	_ = otlptracehttp.New

	// Resource - это "метаданные сервиса". Все атрибуты, добавленные сюда, будут
	// приклеены ко ВСЕМ спанам, которые сгенерит этот процесс. В Jaeger они попадают
	// в блок "Process" и по ним можно делать фильтрацию ("покажи всё из staging").
	//
	// Минимум - service.name. Без него Jaeger тебя не найдёт в дропдауне.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("0.1.0"),
			// Окружение: dev/staging/prod/local. В реале берётся из env.
			semconv.DeploymentEnvironment("local"),
		),
	)
	if err != nil {
		return nil, err
	}

	// TODO 2: создай TracerProvider и зарегистрируй его глобально.
	//
	//   tp := sdktrace.NewTracerProvider(
	//       sdktrace.WithBatcher(exporter),                  // см. ниже про батчинг
	//       sdktrace.WithResource(res),                       // наши метаданные
	//       sdktrace.WithSampler(sdktrace.AlwaysSample()),    // 100% - для учёбы
	//   )
	//   otel.SetTracerProvider(tp)
	//
	// Зачем WithBatcher, а не SimpleSpanProcessor?
	//   SimpleSpanProcessor шлёт каждый спан отдельным HTTP-запросом. На 10k RPS
	//   сервис умрёт быстрее, чем от бизнес-логики. Batcher копит спаны и
	//   отправляет пачками (по таймеру или по размеру буфера) - это стандарт.
	//
	// Зачем AlwaysSample?
	//   Мы учимся, трафика мало, хотим видеть каждый запрос в UI. В проде так
	//   НЕ делают - объём данных огромен, хранить всё дорого. Обычно берут
	//   TraceIDRatioBased(0.01..0.1) - сохраняется 1-10% трейсов.
	//
	// Зачем SetTracerProvider?
	//   Чтобы дальше в коде можно было писать otel.Tracer("...") и получать
	//   трейсер, подключённый к этому провайдеру. Это глобальная синглтон-точка.
	var tp *sdktrace.TracerProvider
	_ = tp
	_ = res

	// Propagator - штука, которая кодирует trace_id в/из HTTP-заголовков.
	// TraceContext = стандарт W3C, заголовок `traceparent`. Его понимают все
	// современные системы. Baggage = дополнительные key-value, которые тоже
	// путешествуют между сервисами.
	//
	// Если это не установить - входящий traceparent будет игнорироваться
	// и trace разорвётся на каждом вызове между сервисами.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Возвращаем shutdown - вызвать defer в main.
	return func(ctx context.Context) error {
		// TODO 3: верни tp.Shutdown(ctx)
		// Shutdown делает три вещи: флашит буфер в экспортер, закрывает экспортер,
		// отключает провайдер. ctx с таймаутом - чтобы не висеть навсегда, если
		// Jaeger уже лёг.
		return nil
	}, nil
}

// =====================================================================
// "Бизнес-логика" - имитация обработки заказа
// =====================================================================

// tracer получаем один раз на пакет. Имя передаётся произвольное, но принято
// класть путь пакета - в Jaeger он показывается в поле "Instrumentation Library",
// помогает отличить "откуда пришёл спан".
var tracer = otel.Tracer("task03-jaeger/order")

// validateInput имитирует валидацию. Важно: принимает ctx первым аргументом -
// это соглашение Go для всего, что может стать спаном, походом в БД и т.д.
func validateInput(ctx context.Context, id string) error {
	// tracer.Start возвращает:
	//   - новый ctx, в котором "активный спан" = наш validateInput
	//   - сам span, на который можно вешать атрибуты
	//
	// Если в этой функции ты вызовешь другую, передав новый ctx, её спан
	// автоматически станет child-ом validateInput. Это и создаёт дерево.
	ctx, span := tracer.Start(ctx, "validateInput")
	// span.End() ОБЯЗАТЕЛЕН. Без него:
	//   - спан не получит длительность,
	//   - он не уйдёт в экспортер,
	//   - в SDK останется ссылка (утечка).
	// defer - самый надёжный способ не забыть, даже при ранних return.
	defer span.End()
	_ = ctx // здесь мы контекст дальше не передаём, но показали, что он есть

	// Атрибут = тэг на спане. В UI по ним можно искать: "все трейсы где order.id=42".
	// attribute.String - одна из многих типобезопасных конструкций (Int, Bool и т.д.).
	span.SetAttributes(attribute.String("order.id", id))

	if strings.TrimSpace(id) == "" {
		err := errors.New("empty order id")

		// TODO 4: правильно пометь спан ошибочным - нужны ОБА вызова.
		//
		//   span.SetStatus(codes.Error, err.Error())
		//   span.RecordError(err)
		//
		// Зачем оба?
		//   SetStatus красит спан красным в UI - визуальный сигнал.
		//   RecordError добавляет внутрь спана "event" с типом ошибки, сообщением
		//     и (если включишь) стеком - чтобы в UI развернуть и посмотреть детали.
		//   Один без другого - либо "красно, но непонятно что", либо "детали есть,
		//   но на глаз ок".
		_ = codes.Error

		return err
	}

	// Имитируем немного работы - 0..10ms. На реальной валидации сложного объекта
	// может быть и больше.
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return nil
}

// callDB - имитация запроса в БД. Специально сделан медленнее, чтобы в trace
// было видно, что именно он занимает большую часть времени.
func callDB(ctx context.Context, id string) (string, error) {
	// WithAttributes при старте - удобный шорткат, чтобы не писать SetAttributes отдельно.
	// semconv.DBSystem, db.statement и т.д. - стандартные ключи. Grafana/Datadog/Jaeger
	// их узнают и умеют красиво отображать.
	ctx, span := tracer.Start(ctx, "callDB",
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "SELECT * FROM orders WHERE id=$1"),
		),
	)
	defer span.End()
	_ = ctx

	// 10..100ms - реалистичная длительность сетевого запроса к БД в том же DC.
	time.Sleep(time.Duration(10+rand.Intn(90)) * time.Millisecond)

	// В 1 случае из 5 - "таймаут". Нужно, чтобы в Jaeger были трейсы с ошибками.
	if rand.Intn(5) == 0 {
		err := errors.New("db timeout")
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}

	return fmt.Sprintf("order-%s", id), nil
}

// =====================================================================
// HTTP
// =====================================================================

func orderHandler(w http.ResponseWriter, r *http.Request) {
	// ВАЖНО: берём именно r.Context(), а не context.Background().
	// В r.Context() уже сидит корневой спан, созданный otelhttp-middleware.
	// Если взять Background - наши спаны окажутся в своём отдельном дереве,
	// никак не связанном с HTTP-запросом. В Jaeger получишь две половинки.
	ctx := r.Context()

	// Простейший парсинг: /order/42 → id = "42"
	id := strings.TrimPrefix(r.URL.Path, "/order/")

	if err := validateInput(ctx, id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	order, err := callDB(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = fmt.Fprintln(w, order)
}

func main() {
	ctx := context.Background()

	shutdown, err := initTracer(ctx)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	// defer вызовет shutdown при завершении main. В проде сюда добавляют
	// graceful-shutdown с сигналами - см. раздел 5.4-observability в курсе.
	defer func() {
		// Отдельный ctx с таймаутом - если Jaeger завис, не висим здесь вечно.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/order/", orderHandler)

	// TODO 5: оберни mux в otelhttp.NewHandler.
	//
	//   handler := otelhttp.NewHandler(mux, "http.server")
	//
	// Второй аргумент - дефолтное имя корневого спана, если middleware не смог
	// определить его по роуту. В Jaeger ты обычно хочешь видеть что-то вроде
	// "GET /order/:id" - для этого используй otelhttp.WithSpanNameFormatter.
	//
	// Когда оборачиваешь ВЕСЬ mux - ты инструментируешь сразу все эндпоинты,
	// не надо оборачивать каждый руками. Минус - в Jaeger будут и /metrics
	// в будущих задачах (пустой шум). В проде обычно выключают трейсинг для
	// health-чеков и /metrics.
	var handler http.Handler = mux
	_ = otelhttp.NewHandler

	addr := ":8082"
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, handler))
}
