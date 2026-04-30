// task01: инструментируем HTTP-сервис метриками Prometheus.
//
// Что в итоге должно получиться: сервис на :8081 с двумя эндпоинтами:
//   GET /hello    - бизнес-хендлер, имитирует работу
//   GET /metrics  - тут живут метрики, сюда ходит Prometheus
//
// Запуск:
//   go mod tidy
//   go run main.go

package main

import (
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	// Прометеевский клиент для Go. Разбит на подпакеты неспроста:
	//
	//   prometheus - сам API: типы метрик (CounterVec, HistogramVec), Registry,
	//                чтобы создавать и регистрировать метрики вручную.
	//
	//   promauto  - обёртка, которая при создании метрики ещё и сразу регистрирует
	//                её в глобальном реестре. Меньше бойлерплейта, пишем её здесь.
	//                В больших проектах часто берут чистый prometheus.New*()
	//                и свой Registry, чтобы изолировать метрики между модулями.
	//
	//   promhttp  - готовый http.Handler, умеет сериализовать метрики в формат,
	//                который понимает Prometheus. Без него пришлось бы вручную
	//                обходить регистр и писать текст - не нужно, всё уже сделано.
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// =====================================================================
// Метрики
// =====================================================================

// TODO 1: объяви httpRequests - *prometheus.CounterVec.
//
// Зачем именно CounterVec, а не Counter?
//   Counter - одна метрика, одно значение. Для всего-всего счётчика запросов сразу.
//   CounterVec - семейство счётчиков, по одному на каждую комбинацию лейблов.
//   Нам нужно различать GET/POST и разные пути, поэтому Vec.
//
// Что сделать:
//
//   var httpRequests = promauto.NewCounterVec(
//       prometheus.CounterOpts{
//           Name: "http_requests_total",
//           // Суффикс "_total" - соглашение Prometheus для счётчиков.
//           // В Grafana и alertmanager-е это важно - некоторые плагины понимают это.
//           Help: "Количество HTTP-запросов по методу, пути и коду ответа.",
//       },
//       // Порядок лейблов важен! Когда будем вызывать .WithLabelValues(a, b, c) -
//       // это будет именно этот порядок. Перепутаешь - данные уедут не туда.
//       []string{"method", "path", "status"},
//   )
//
// Почему такие лейблы:
//   method - GET/POST/... у нас их меньше 10, безопасно.
//   path   - имя эндпоинта. ВАЖНО: если у тебя роуты типа /users/42 и /users/43 -
//            НЕ клади сюда сам URL. Клади шаблон "/users/:id". Иначе получишь
//            по одному ряду на каждого юзера и Prometheus умрёт.
//   status - HTTP-код (200, 500, 404...). Десятки значений - ок.
//
// Замени nil на свою метрику.
var httpRequests *prometheus.CounterVec

// TODO 2: объяви httpDuration - *prometheus.HistogramVec.
//
// Histogram нужен чтобы потом в PromQL считать p95/p99 латентности.
// Под капотом он складирует длительности по бакетам - "сколько запросов
// уложилось в <=5ms, сколько в <=10ms, ..." - и Prometheus эти числа
// складывает со всех реплик, а уже потом histogram_quantile() считает
// настоящий перцентиль.
//
// Что сделать:
//
//   var httpDuration = promauto.NewHistogramVec(
//       prometheus.HistogramOpts{
//           Name: "http_request_duration_seconds",
//           // Хорошая практика - включать единицу измерения в имя.
//           // Без _seconds ты через полгода не вспомнишь, там миллисекунды или секунды.
//           Help: "Длительность обработки HTTP-запроса, в секундах.",
//
//           // Бакеты - это пороги, по которым раскладываются измерения.
//           // DefBuckets: .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10 секунд.
//           // Это норм для обычного веб-сервиса.
//           //
//           // А если у тебя сервис с субмиллисекундными ответами (кеш)?
//           // Бакеты от 5ms будут бесполезны - всё попадёт в самый нижний.
//           // Тогда замени на что-то вроде:
//           //   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1}
//           Buckets: prometheus.DefBuckets,
//       },
//       // Те же лейблы, что у Counter-а. Так удобно джойнить метрики в PromQL:
//       // "RPS и p95 по одному и тому же разрезу лейблов".
//       []string{"method", "path", "status"},
//   )
var httpDuration *prometheus.HistogramVec

// =====================================================================
// Middleware - измеряем каждый запрос
// =====================================================================

// Зачем нам свой ResponseWriter?
//
// Стандартный http.ResponseWriter после записи ответа не даёт узнать, какой был
// статус-код. А нам он нужен, чтобы записать его в лейбл.
//
// Трюк стандартный: оборачиваем writer, перехватываем WriteHeader и запоминаем код.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	// Важно прокинуть вызов в настоящий writer, иначе клиент не увидит код.
	r.ResponseWriter.WriteHeader(code)
}

// metricsMiddleware - обёртка, которая на каждый запрос:
//  1) засекает время старта
//  2) пропускает запрос в настоящий хендлер
//  3) считает длительность и пишет её в Histogram, а также +1 в Counter
//
// Почему middleware, а не вызов внутри каждого хендлера?
//   Потому что если у тебя 50 хендлеров - ты не хочешь в каждом дублировать три
//   строчки про метрики. И главное - легко забыть в одном из них, и слепое пятно.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// time.Now() здесь, до next.ServeHTTP - именно начало обработки.
		start := time.Now()

		// Дефолтный статус 200. Если хендлер вообще не вызовет WriteHeader,
		// Go за него пошлёт 200 - и у нас в метрике тоже должно быть 200.
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		// Вот собственно вызов настоящего хендлера. Всё, что он запишет,
		// пройдёт через наш statusRecorder.
		next.ServeHTTP(rec, r)

		// TODO 3: после обработки запроса посчитай длительность и обнови обе метрики.
		//
		//   duration := time.Since(start).Seconds()
		//   status := strconv.Itoa(rec.status)
		//
		//   // Inc() атомарно прибавляет 1 к счётчику с нужной комбинацией лейблов.
		//   // Если такой комбинации ещё не было - она создастся при первом вызове.
		//   httpRequests.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		//
		//   // Observe() кладёт значение в гистограмму - распределит по бакетам.
		//   httpDuration.WithLabelValues(r.Method, r.URL.Path, status).Observe(duration)
		//
		// Почему .Seconds(), а не .Milliseconds()?
		//   Соглашение Prometheus - всегда секунды. Так работают все встроенные
		//   функции типа histogram_quantile, так сделаны DefBuckets. Не борись с этим.
		//
		// Почему r.URL.Path - это ОК здесь, но опасно в реальном роутере?
		//   У нас всего два эндпоинта с фиксированными путями. В реальном роутере
		//   (chi, gin) надо брать не Path, а шаблон роута - иначе /users/42 и
		//   /users/43 создадут два разных ряда.
		_ = start
		_ = rec
		_ = strconv.Itoa
	})
}

// =====================================================================
// Хендлеры
// =====================================================================

// helloHandler - очень простой бизнес-эндпоинт. Две фишки:
//  - случайная задержка 0..100ms - чтобы Histogram не был пустым
//  - иногда 500 - чтобы в метриках были ошибки и было что смотреть в error-rate
func helloHandler(w http.ResponseWriter, r *http.Request) {
	// math/rand/v2 в Go 1.22+ уже thread-safe без явной инициализации seed.
	// rand.Intn(100) - случайное число 0..99.
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	// Каждый 10-й запрос - ошибка. Позже в Grafana увидим error rate 10%.
	if rand.Intn(10) == 0 {
		http.Error(w, "oops", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write([]byte("hello\n"))
}

// =====================================================================
// main
// =====================================================================

func main() {
	// ServeMux - это встроенный роутер. Маленький, но нам его хватит.
	// В проде обычно chi или gin, но принцип тот же.
	mux := http.NewServeMux()

	// TODO 4: зарегистрируй бизнес-хендлер через middleware.
	//
	//   mux.Handle("/hello", metricsMiddleware(http.HandlerFunc(helloHandler)))
	//
	// Что тут происходит:
	//   http.HandlerFunc(helloHandler) - превращаем функцию в http.Handler
	//   metricsMiddleware(...)         - оборачиваем в middleware с метриками
	//   mux.Handle(...)                - регистрируем на пути /hello
	//
	// Если захочешь ещё эндпоинт /slow с задержкой побольше - просто зарегистрируй
	// так же. В метриках они автоматически разделятся лейблом path.
	_ = helloHandler
	_ = promauto.NewCounterVec // просто чтобы Go не ругался на неиспользованный promauto

	// TODO 5: отдай /metrics через promhttp.Handler().
	//
	//   mux.Handle("/metrics", promhttp.Handler())
	//
	// promhttp.Handler() берёт все метрики из глобального Registry и сериализует
	// их в специальный текстовый формат. Именно его скрейпит Prometheus.
	//
	// ВАЖНО: /metrics НЕ надо оборачивать в metricsMiddleware. Иначе каждый скрейп
	// сам по себе будет инкрементить счётчик - получишь чудовищное искажение данных.
	_ = promhttp.Handler

	addr := ":8081"
	log.Printf("listening on %s", addr)

	// http.ListenAndServe блокирующий, не вернётся пока сервер не упадёт.
	// В продакшене сюда добавляют graceful shutdown (это в 5.4-observability),
	// но для задачи достаточно.
	log.Fatal(http.ListenAndServe(addr, mux))
}
