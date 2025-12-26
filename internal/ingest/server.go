package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reductrai/agent/internal/storage"
	"github.com/reductrai/agent/pkg/types"
)

// Default buffer and batch sizes
const (
	DefaultBufferSize    = 100000 // 100K items per channel
	DefaultBatchSize     = 1000   // Insert 1000 at a time
	MinBatchSize         = 100    // Minimum batch size
	MaxBatchSize         = 10000  // Maximum batch size
	FlushInterval        = 100 * time.Millisecond
	ScaleCheckInterval   = 1 * time.Second
	HighPressureThreshold = 0.7  // 70% buffer full = scale up
	LowPressureThreshold  = 0.2  // 20% buffer full = scale down
)

// Server handles telemetry ingestion with high-throughput buffering
type Server struct {
	port    int
	storage *storage.DuckDB
	server  *http.Server
	stats   IngestStats

	// Buffered channels for async ingestion
	spanChan   chan types.Span
	logChan    chan types.LogEntry
	metricChan chan types.Metric

	// Configuration (initial values, auto-scaled at runtime)
	bufferSize int
	batchSize  int

	// Dynamic batch sizes (auto-scaled based on pressure)
	spanBatchSize   int32
	logBatchSize    int32
	metricBatchSize int32

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// IngestStats tracks ingestion metrics
type IngestStats struct {
	MetricsReceived int64
	LogsReceived    int64
	SpansReceived   int64
	MetricsQueued   int64
	LogsQueued      int64
	SpansQueued     int64
	BatchesWritten  int64
	LastIngest      time.Time
}

// NewServer creates a new ingestion server with high-throughput buffering
func NewServer(port int, storage *storage.DuckDB) *Server {
	return &Server{
		port:       port,
		storage:    storage,
		bufferSize: DefaultBufferSize,
		batchSize:  DefaultBatchSize,
	}
}

// NewServerWithConfig creates a new ingestion server with custom buffer sizes
func NewServerWithConfig(port int, storage *storage.DuckDB, bufferSize, batchSize int) *Server {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &Server{
		port:       port,
		storage:    storage,
		bufferSize: bufferSize,
		batchSize:  batchSize,
	}
}

// Start starts the HTTP server with background batch workers
func (s *Server) Start(ctx context.Context) error {
	// Create internal context for workers
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Initialize buffered channels
	s.spanChan = make(chan types.Span, s.bufferSize)
	s.logChan = make(chan types.LogEntry, s.bufferSize)
	s.metricChan = make(chan types.Metric, s.bufferSize)

	// Initialize dynamic batch sizes
	atomic.StoreInt32(&s.spanBatchSize, int32(s.batchSize))
	atomic.StoreInt32(&s.logBatchSize, int32(s.batchSize))
	atomic.StoreInt32(&s.metricBatchSize, int32(s.batchSize))

	// Start background batch workers
	s.wg.Add(3)
	go s.spanBatchWorker()
	go s.logBatchWorker()
	go s.metricBatchWorker()

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", s.handleHealth)

	// Generic ingestion (auto-detect format)
	mux.HandleFunc("/", s.handleIngest)

	// Format-specific endpoints
	// === OpenTelemetry ===
	mux.HandleFunc("/v1/traces", s.handleOTLPTraces)          // OTLP traces
	mux.HandleFunc("/v1/logs", s.handleOTLPLogs)              // OTLP logs
	mux.HandleFunc("/v1/metrics", s.handleOTLPMetrics)        // OTLP metrics

	// === Prometheus ===
	mux.HandleFunc("/api/v1/prom/write", s.handlePrometheus)  // Prometheus remote write
	mux.HandleFunc("/api/v1/write", s.handlePrometheus)       // Prometheus remote write (alt)
	mux.HandleFunc("/prometheus/api/v1/write", s.handlePrometheus)

	// === Datadog ===
	mux.HandleFunc("/api/v2/series", s.handleDatadog)         // Datadog metrics
	mux.HandleFunc("/api/v1/series", s.handleDatadog)         // Datadog metrics (v1)
	mux.HandleFunc("/api/v1/check_run", s.handleDatadog)      // Datadog service checks
	mux.HandleFunc("/api/v2/logs", s.handleDatadogLogs)       // Datadog logs v2
	mux.HandleFunc("/api/v1/logs", s.handleDatadogLogs)       // Datadog logs v1
	mux.HandleFunc("/v1/input", s.handleDatadogLogs)          // Datadog logs (alt)

	// === Jaeger ===
	mux.HandleFunc("/api/traces", s.handleJaeger)             // Jaeger traces
	mux.HandleFunc("/jaeger/api/traces", s.handleJaeger)      // Jaeger (prefixed)

	// === Zipkin ===
	mux.HandleFunc("/api/v2/spans", s.handleZipkin)           // Zipkin v2 traces
	mux.HandleFunc("/api/v1/spans", s.handleZipkin)           // Zipkin v1 traces
	mux.HandleFunc("/zipkin/api/v2/spans", s.handleZipkin)    // Zipkin (prefixed)

	// === Grafana/Loki ===
	mux.HandleFunc("/loki/api/v1/push", s.handleLoki)         // Loki logs

	// === Splunk ===
	mux.HandleFunc("/services/collector/event", s.handleSplunkHEC)  // Splunk HEC
	mux.HandleFunc("/services/collector", s.handleSplunkHEC)        // Splunk HEC (alt)

	// === Elasticsearch ===
	mux.HandleFunc("/_bulk", s.handleElasticsearch)                 // Elasticsearch bulk
	mux.HandleFunc("/elasticsearch/_bulk", s.handleElasticsearch)   // Elasticsearch (prefixed)

	// === InfluxDB ===
	mux.HandleFunc("/write", s.handleInfluxDB)                      // InfluxDB v1
	mux.HandleFunc("/api/v2/write", s.handleInfluxDB)               // InfluxDB v2
	mux.HandleFunc("/influx/write", s.handleInfluxDB)               // InfluxDB (prefixed)

	// === Graphite ===
	mux.HandleFunc("/graphite/metrics", s.handleGraphite)           // Graphite metrics
	mux.HandleFunc("/events/", s.handleGraphite)                    // Graphite events

	// === StatsD (HTTP) ===
	mux.HandleFunc("/statsd", s.handleStatsD)                       // StatsD over HTTP
	mux.HandleFunc("/telegraf/statsd", s.handleStatsD)              // StatsD via Telegraf

	// === Fluent Bit / Fluentd ===
	mux.HandleFunc("/fluent", s.handleFluentd)                      // Fluent forward
	mux.HandleFunc("/fluentd", s.handleFluentd)                     // Fluentd
	mux.HandleFunc("/td-agent", s.handleFluentd)                    // td-agent (Fluentd)

	// === Syslog ===
	mux.HandleFunc("/syslog", s.handleSyslog)                       // Syslog over HTTP

	// === New Relic ===
	mux.HandleFunc("/log/v1", s.handleNewRelicLogs)                 // New Relic logs
	mux.HandleFunc("/metric/v1", s.handleNewRelicMetrics)           // New Relic metrics
	mux.HandleFunc("/trace/v1", s.handleNewRelicTraces)             // New Relic traces

	// === Honeycomb ===
	mux.HandleFunc("/1/events/", s.handleHoneycomb)                 // Honeycomb events
	mux.HandleFunc("/1/batch/", s.handleHoneycomb)                  // Honeycomb batch

	// === AWS ===
	mux.HandleFunc("/xray", s.handleAWSXRay)                        // AWS X-Ray
	mux.HandleFunc("/cloudwatch", s.handleCloudWatch)               // CloudWatch metrics
	mux.HandleFunc("/firehose", s.handleCloudWatch)                 // Kinesis Firehose

	// === Google Cloud ===
	mux.HandleFunc("/v2/traces", s.handleGoogleCloudTrace)          // Google Cloud Trace
	mux.HandleFunc("/google/logging", s.handleGoogleCloudLogging)   // Google Cloud Logging

	// === Azure ===
	mux.HandleFunc("/azure/metrics", s.handleAzureMonitor)          // Azure Monitor
	mux.HandleFunc("/azure/logs", s.handleAzureMonitor)             // Azure Logs

	// === Dynatrace ===
	mux.HandleFunc("/api/v2/metrics/ingest", s.handleDynatrace)     // Dynatrace metrics
	mux.HandleFunc("/api/v2/logs/ingest", s.handleDynatrace)        // Dynatrace logs

	// === AppDynamics ===
	mux.HandleFunc("/appdynamics/events", s.handleAppDynamics)      // AppDynamics

	// === SignalFx/Splunk Observability ===
	mux.HandleFunc("/v2/datapoint", s.handleSignalFx)               // SignalFx metrics
	mux.HandleFunc("/v2/event", s.handleSignalFx)                   // SignalFx events

	// === Logstash ===
	mux.HandleFunc("/logstash", s.handleLogstash)                   // Logstash HTTP input

	// === Sumo Logic ===
	mux.HandleFunc("/receiver/v1/http", s.handleSumoLogic)          // Sumo Logic HTTP

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		s.shutdown()
	}()

	err := s.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// shutdown gracefully shuts down the server and flushes buffers
func (s *Server) shutdown() {
	// Stop accepting new requests
	s.server.Shutdown(context.Background())

	// Signal workers to stop
	s.cancel()

	// Wait for workers to drain buffers
	s.wg.Wait()
}

// spanBatchWorker drains span channel and batch inserts with auto-scaling
func (s *Server) spanBatchWorker() {
	defer s.wg.Done()

	batch := make([]types.Span, 0, MaxBatchSize)
	ticker := time.NewTicker(FlushInterval)
	scaleTicker := time.NewTicker(ScaleCheckInterval)
	defer ticker.Stop()
	defer scaleTicker.Stop()

	flush := func() {
		if len(batch) > 0 {
			if err := s.storage.InsertSpansBatch(batch); err != nil {
				fmt.Printf("[Ingest] Failed to insert span batch: %v\n", err)
			} else {
				atomic.AddInt64(&s.stats.BatchesWritten, 1)
			}
			batch = batch[:0]
		}
	}

	autoScale := func() {
		queueLen := len(s.spanChan)
		pressure := float64(queueLen) / float64(s.bufferSize)
		currentBatch := atomic.LoadInt32(&s.spanBatchSize)

		if pressure > HighPressureThreshold {
			// Scale up batch size to drain faster
			newBatch := int32(float64(currentBatch) * 1.5)
			if newBatch > MaxBatchSize {
				newBatch = MaxBatchSize
			}
			atomic.StoreInt32(&s.spanBatchSize, newBatch)
		} else if pressure < LowPressureThreshold && currentBatch > MinBatchSize {
			// Scale down to reduce latency
			newBatch := int32(float64(currentBatch) * 0.8)
			if newBatch < MinBatchSize {
				newBatch = MinBatchSize
			}
			atomic.StoreInt32(&s.spanBatchSize, newBatch)
		}
	}

	for {
		currentBatchSize := int(atomic.LoadInt32(&s.spanBatchSize))
		select {
		case span, ok := <-s.spanChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, span)
			if len(batch) >= currentBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-scaleTicker.C:
			autoScale()
		case <-s.ctx.Done():
			// Drain remaining items
			for {
				select {
				case span := <-s.spanChan:
					batch = append(batch, span)
					if len(batch) >= currentBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// logBatchWorker drains log channel and batch inserts with auto-scaling
func (s *Server) logBatchWorker() {
	defer s.wg.Done()

	batch := make([]types.LogEntry, 0, MaxBatchSize)
	ticker := time.NewTicker(FlushInterval)
	scaleTicker := time.NewTicker(ScaleCheckInterval)
	defer ticker.Stop()
	defer scaleTicker.Stop()

	flush := func() {
		if len(batch) > 0 {
			if err := s.storage.InsertLogsBatch(batch); err != nil {
				fmt.Printf("[Ingest] Failed to insert log batch: %v\n", err)
			} else {
				atomic.AddInt64(&s.stats.BatchesWritten, 1)
			}
			batch = batch[:0]
		}
	}

	autoScale := func() {
		queueLen := len(s.logChan)
		pressure := float64(queueLen) / float64(s.bufferSize)
		currentBatch := atomic.LoadInt32(&s.logBatchSize)

		if pressure > HighPressureThreshold {
			newBatch := int32(float64(currentBatch) * 1.5)
			if newBatch > MaxBatchSize {
				newBatch = MaxBatchSize
			}
			atomic.StoreInt32(&s.logBatchSize, newBatch)
		} else if pressure < LowPressureThreshold && currentBatch > MinBatchSize {
			newBatch := int32(float64(currentBatch) * 0.8)
			if newBatch < MinBatchSize {
				newBatch = MinBatchSize
			}
			atomic.StoreInt32(&s.logBatchSize, newBatch)
		}
	}

	for {
		currentBatchSize := int(atomic.LoadInt32(&s.logBatchSize))
		select {
		case log, ok := <-s.logChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, log)
			if len(batch) >= currentBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-scaleTicker.C:
			autoScale()
		case <-s.ctx.Done():
			// Drain remaining items
			for {
				select {
				case log := <-s.logChan:
					batch = append(batch, log)
					if len(batch) >= currentBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// metricBatchWorker drains metric channel and batch inserts with auto-scaling
func (s *Server) metricBatchWorker() {
	defer s.wg.Done()

	batch := make([]types.Metric, 0, MaxBatchSize)
	ticker := time.NewTicker(FlushInterval)
	scaleTicker := time.NewTicker(ScaleCheckInterval)
	defer ticker.Stop()
	defer scaleTicker.Stop()

	flush := func() {
		if len(batch) > 0 {
			if err := s.storage.InsertMetricsBatch(batch); err != nil {
				fmt.Printf("[Ingest] Failed to insert metric batch: %v\n", err)
			} else {
				atomic.AddInt64(&s.stats.BatchesWritten, 1)
			}
			batch = batch[:0]
		}
	}

	autoScale := func() {
		queueLen := len(s.metricChan)
		pressure := float64(queueLen) / float64(s.bufferSize)
		currentBatch := atomic.LoadInt32(&s.metricBatchSize)

		if pressure > HighPressureThreshold {
			newBatch := int32(float64(currentBatch) * 1.5)
			if newBatch > MaxBatchSize {
				newBatch = MaxBatchSize
			}
			atomic.StoreInt32(&s.metricBatchSize, newBatch)
		} else if pressure < LowPressureThreshold && currentBatch > MinBatchSize {
			newBatch := int32(float64(currentBatch) * 0.8)
			if newBatch < MinBatchSize {
				newBatch = MinBatchSize
			}
			atomic.StoreInt32(&s.metricBatchSize, newBatch)
		}
	}

	for {
		currentBatchSize := int(atomic.LoadInt32(&s.metricBatchSize))
		select {
		case metric, ok := <-s.metricChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, metric)
			if len(batch) >= currentBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-scaleTicker.C:
			autoScale()
		case <-s.ctx.Done():
			// Drain remaining items
			for {
				select {
				case metric := <-s.metricChan:
					batch = append(batch, metric)
					if len(batch) >= currentBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// queueSpan adds a span to the buffer (non-blocking with backpressure)
func (s *Server) queueSpan(span types.Span) bool {
	select {
	case s.spanChan <- span:
		atomic.AddInt64(&s.stats.SpansQueued, 1)
		return true
	default:
		// Buffer full - apply backpressure
		return false
	}
}

// queueLog adds a log to the buffer (non-blocking with backpressure)
func (s *Server) queueLog(log types.LogEntry) bool {
	select {
	case s.logChan <- log:
		atomic.AddInt64(&s.stats.LogsQueued, 1)
		return true
	default:
		return false
	}
}

// queueMetric adds a metric to the buffer (non-blocking with backpressure)
func (s *Server) queueMetric(metric types.Metric) bool {
	select {
	case s.metricChan <- metric:
		atomic.AddInt64(&s.stats.MetricsQueued, 1)
		return true
	default:
		return false
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Calculate buffer usage
	spanBufferUsed := 0
	logBufferUsed := 0
	metricBufferUsed := 0
	if s.spanChan != nil {
		spanBufferUsed = len(s.spanChan)
	}
	if s.logChan != nil {
		logBufferUsed = len(s.logChan)
	}
	if s.metricChan != nil {
		metricBufferUsed = len(s.metricChan)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"stats":  s.stats,
		"buffers": map[string]interface{}{
			"spans":      spanBufferUsed,
			"logs":       logBufferUsed,
			"metrics":    metricBufferUsed,
			"bufferSize": s.bufferSize,
		},
		"autoScale": map[string]interface{}{
			"spanBatchSize":   atomic.LoadInt32(&s.spanBatchSize),
			"logBatchSize":    atomic.LoadInt32(&s.logBatchSize),
			"metricBatchSize": atomic.LoadInt32(&s.metricBatchSize),
			"minBatchSize":    MinBatchSize,
			"maxBatchSize":    MaxBatchSize,
		},
	})
}

// handleIngest auto-detects format and routes appropriately
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	userAgent := r.Header.Get("User-Agent")

	// Auto-detect format
	format := s.detectFormat(r.URL.Path, contentType, userAgent, body)

	switch format {
	case "otlp":
		s.ingestOTLP(body, w)
	case "prometheus":
		s.ingestPrometheus(body, w)
	case "datadog":
		s.ingestDatadog(body, w)
	case "jaeger":
		s.ingestJaeger(body, w)
	case "zipkin":
		s.ingestZipkin(body, w)
	case "loki":
		s.ingestLoki(body, w)
	case "json":
		s.ingestGenericJSON(body, w)
	default:
		// Try generic JSON
		s.ingestGenericJSON(body, w)
	}
}

func (s *Server) detectFormat(path, contentType, userAgent string, body []byte) string {
	// Check path
	if strings.Contains(path, "/otlp") || strings.Contains(path, "/v1/traces") {
		return "otlp"
	}
	if strings.Contains(path, "/prometheus") || strings.Contains(path, "/metrics") {
		return "prometheus"
	}
	if strings.Contains(path, "/datadog") || strings.Contains(userAgent, "datadog") {
		return "datadog"
	}
	if strings.Contains(path, "/jaeger") || strings.Contains(userAgent, "jaeger") {
		return "jaeger"
	}
	if strings.Contains(path, "/zipkin") || strings.Contains(userAgent, "zipkin") {
		return "zipkin"
	}
	if strings.Contains(path, "/loki") {
		return "loki"
	}

	// Check content type
	if strings.Contains(contentType, "application/x-protobuf") {
		return "otlp"
	}

	// Check body structure
	if len(body) > 0 {
		if body[0] == '{' {
			// JSON - try to detect structure
			var probe map[string]interface{}
			if json.Unmarshal(body, &probe) == nil {
				if _, ok := probe["resourceSpans"]; ok {
					return "otlp"
				}
				if _, ok := probe["series"]; ok {
					return "datadog"
				}
				// Jaeger format has "data" array with spans
				if _, ok := probe["data"]; ok {
					return "jaeger"
				}
				// Loki format has "streams" array
				if _, ok := probe["streams"]; ok {
					return "loki"
				}
			}
		}
		// Zipkin is an array of spans
		if body[0] == '[' {
			var probe []map[string]interface{}
			if json.Unmarshal(body, &probe) == nil && len(probe) > 0 {
				// Zipkin spans have localEndpoint
				if _, ok := probe[0]["localEndpoint"]; ok {
					return "zipkin"
				}
			}
		}
	}

	return "json"
}

func (s *Server) handlePrometheus(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestPrometheus(body, w)
}

func (s *Server) handleDatadog(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestDatadog(body, w)
}

func (s *Server) handleDatadogLogs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestDatadogLogs(body, w)
}

func (s *Server) handleOTLPTraces(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestOTLP(body, w)
}

func (s *Server) handleOTLPLogs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestOTLPLogs(body, w)
}

func (s *Server) handleOTLPMetrics(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestOTLPMetrics(body, w)
}

// ingestPrometheus handles Prometheus remote write format (async with 202 Accepted)
func (s *Server) ingestPrometheus(body []byte, w http.ResponseWriter) {
	// Simplified: parse Prometheus text format or remote write
	// In production, use prometheus/common/model
	atomic.AddInt64(&s.stats.MetricsReceived, 1)
	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// ingestDatadog handles Datadog metrics (async with 202 Accepted)
func (s *Server) ingestDatadog(body []byte, w http.ResponseWriter) {
	var payload struct {
		Series []struct {
			Metric string      `json:"metric"`
			Points [][]float64 `json:"points"`
			Host   string      `json:"host"`
			Tags   []string    `json:"tags"`
		} `json:"series"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Datadog payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, series := range payload.Series {
		for _, point := range series.Points {
			if len(point) >= 2 {
				m := types.Metric{
					Name:      series.Metric,
					Value:     point[1],
					Timestamp: time.Unix(int64(point[0]), 0),
					Service:   series.Host,
					Tags:      tagsToMap(series.Tags),
				}
				if s.queueMetric(m) {
					queued++
				} else {
					dropped++
				}
				atomic.AddInt64(&s.stats.MetricsReceived, 1)
			}
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

// ingestDatadogLogs handles Datadog logs (async with 202 Accepted)
func (s *Server) ingestDatadogLogs(body []byte, w http.ResponseWriter) {
	var logs []struct {
		Message  string `json:"message"`
		DDSource string `json:"ddsource"`
		DDTags   string `json:"ddtags"`
		Hostname string `json:"hostname"`
		Service  string `json:"service"`
	}

	if err := json.Unmarshal(body, &logs); err != nil {
		http.Error(w, "Invalid Datadog logs payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, log := range logs {
		l := types.LogEntry{
			Timestamp: time.Now(),
			Level:     "info",
			Message:   log.Message,
			Service:   log.Service,
			Tags:      map[string]string{"hostname": log.Hostname, "source": log.DDSource},
		}
		if s.queueLog(l) {
			queued++
		} else {
			dropped++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

// ingestOTLP handles OTLP traces (async with 202 Accepted)
func (s *Server) ingestOTLP(body []byte, w http.ResponseWriter) {
	// Simplified OTLP JSON parsing
	var payload struct {
		ResourceSpans []struct {
			Resource struct {
				Attributes []struct {
					Key   string      `json:"key"`
					Value interface{} `json:"value"`
				} `json:"attributes"`
			} `json:"resource"`
			ScopeSpans []struct {
				Spans []struct {
					TraceID           string `json:"traceId"`
					SpanID            string `json:"spanId"`
					ParentSpanID      string `json:"parentSpanId"`
					Name              string `json:"name"`
					StartTimeUnixNano string `json:"startTimeUnixNano"`
					EndTimeUnixNano   string `json:"endTimeUnixNano"`
					Status            struct {
						Code string `json:"code"`
					} `json:"status"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid OTLP payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, rs := range payload.ResourceSpans {
		service := "unknown"
		for _, attr := range rs.Resource.Attributes {
			if attr.Key == "service.name" {
				if v, ok := attr.Value.(map[string]interface{}); ok {
					if sv, ok := v["stringValue"].(string); ok {
						service = sv
					}
				}
			}
		}

		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				sp := types.Span{
					TraceID:      span.TraceID,
					SpanID:       span.SpanID,
					ParentSpanID: span.ParentSpanID,
					Service:      service,
					Operation:    span.Name,
					StartTime:    time.Now(), // Simplified
					Duration:     time.Millisecond * 100, // Simplified
					Status:       span.Status.Code,
				}
				if s.queueSpan(sp) {
					queued++
				} else {
					dropped++
				}
				atomic.AddInt64(&s.stats.SpansReceived, 1)
			}
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

func (s *Server) ingestOTLPLogs(body []byte, w http.ResponseWriter) {
	atomic.AddInt64(&s.stats.LogsReceived, 1)
	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func (s *Server) ingestOTLPMetrics(body []byte, w http.ResponseWriter) {
	atomic.AddInt64(&s.stats.MetricsReceived, 1)
	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// ingestGenericJSON handles generic JSON telemetry (async with 202 Accepted)
func (s *Server) ingestGenericJSON(body []byte, w http.ResponseWriter) {
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	queued := false
	// Try to detect and store appropriately
	if _, ok := data["message"]; ok {
		// Looks like a log
		l := types.LogEntry{
			Timestamp: time.Now(),
			Level:     getString(data, "level", "info"),
			Message:   getString(data, "message", ""),
			Service:   getString(data, "service", "unknown"),
		}
		queued = s.queueLog(l)
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	} else if _, ok := data["value"]; ok {
		// Looks like a metric
		m := types.Metric{
			Name:      getString(data, "name", "unknown"),
			Value:     getFloat(data, "value", 0),
			Timestamp: time.Now(),
			Service:   getString(data, "service", "unknown"),
		}
		queued = s.queueMetric(m)
		atomic.AddInt64(&s.stats.MetricsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "accepted",
		"queued": queued,
	})
}

func tagsToMap(tags []string) map[string]string {
	m := make(map[string]string)
	for _, tag := range tags {
		parts := strings.SplitN(tag, ":", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func getString(m map[string]interface{}, key, def string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return def
}

func getFloat(m map[string]interface{}, key string, def float64) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return def
}

// ============================================================
// Jaeger Support
// ============================================================

func (s *Server) handleJaeger(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestJaeger(body, w)
}

// ingestJaeger handles Jaeger traces (Thrift JSON format)
func (s *Server) ingestJaeger(body []byte, w http.ResponseWriter) {
	// Jaeger JSON format (simplified)
	var payload struct {
		Data []struct {
			TraceID   string `json:"traceID"`
			Spans     []struct {
				TraceID       string `json:"traceID"`
				SpanID        string `json:"spanID"`
				OperationName string `json:"operationName"`
				References    []struct {
					RefType string `json:"refType"`
					TraceID string `json:"traceID"`
					SpanID  string `json:"spanID"`
				} `json:"references"`
				StartTime int64 `json:"startTime"` // microseconds
				Duration  int64 `json:"duration"`  // microseconds
				Tags      []struct {
					Key   string      `json:"key"`
					Type  string      `json:"type"`
					Value interface{} `json:"value"`
				} `json:"tags"`
				Logs []struct {
					Timestamp int64 `json:"timestamp"`
					Fields    []struct {
						Key   string      `json:"key"`
						Type  string      `json:"type"`
						Value interface{} `json:"value"`
					} `json:"fields"`
				} `json:"logs"`
			} `json:"spans"`
			Processes map[string]struct {
				ServiceName string `json:"serviceName"`
				Tags        []struct {
					Key   string      `json:"key"`
					Type  string      `json:"type"`
					Value interface{} `json:"value"`
				} `json:"tags"`
			} `json:"processes"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Jaeger payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, trace := range payload.Data {
		// Get service name from first process
		service := "unknown"
		for _, proc := range trace.Processes {
			service = proc.ServiceName
			break
		}

		for _, span := range trace.Spans {
			// Get parent span ID from references
			parentSpanID := ""
			for _, ref := range span.References {
				if ref.RefType == "CHILD_OF" {
					parentSpanID = ref.SpanID
					break
				}
			}

			// Check for error status in tags
			status := "OK"
			for _, tag := range span.Tags {
				if tag.Key == "error" {
					if v, ok := tag.Value.(bool); ok && v {
						status = "ERROR"
					}
				}
			}

			sp := types.Span{
				TraceID:      span.TraceID,
				SpanID:       span.SpanID,
				ParentSpanID: parentSpanID,
				Service:      service,
				Operation:    span.OperationName,
				StartTime:    time.UnixMicro(span.StartTime),
				Duration:     time.Duration(span.Duration) * time.Microsecond,
				Status:       status,
			}
			if s.queueSpan(sp) {
				queued++
			} else {
				dropped++
			}
			atomic.AddInt64(&s.stats.SpansReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

// ============================================================
// Zipkin Support
// ============================================================

func (s *Server) handleZipkin(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestZipkin(body, w)
}

// ingestZipkin handles Zipkin v2 JSON traces
func (s *Server) ingestZipkin(body []byte, w http.ResponseWriter) {
	// Zipkin v2 JSON format
	var spans []struct {
		TraceID       string `json:"traceId"`
		ID            string `json:"id"`
		ParentID      string `json:"parentId,omitempty"`
		Name          string `json:"name"`
		Timestamp     int64  `json:"timestamp"` // microseconds
		Duration      int64  `json:"duration"`  // microseconds
		Kind          string `json:"kind"`      // CLIENT, SERVER, PRODUCER, CONSUMER
		LocalEndpoint struct {
			ServiceName string `json:"serviceName"`
			IPv4        string `json:"ipv4,omitempty"`
			Port        int    `json:"port,omitempty"`
		} `json:"localEndpoint"`
		RemoteEndpoint struct {
			ServiceName string `json:"serviceName,omitempty"`
			IPv4        string `json:"ipv4,omitempty"`
			Port        int    `json:"port,omitempty"`
		} `json:"remoteEndpoint,omitempty"`
		Tags map[string]string `json:"tags,omitempty"`
	}

	if err := json.Unmarshal(body, &spans); err != nil {
		http.Error(w, "Invalid Zipkin payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, span := range spans {
		// Check for error status in tags
		status := "OK"
		if errVal, ok := span.Tags["error"]; ok && errVal != "" {
			status = "ERROR"
		}

		service := span.LocalEndpoint.ServiceName
		if service == "" {
			service = "unknown"
		}

		sp := types.Span{
			TraceID:      span.TraceID,
			SpanID:       span.ID,
			ParentSpanID: span.ParentID,
			Service:      service,
			Operation:    span.Name,
			StartTime:    time.UnixMicro(span.Timestamp),
			Duration:     time.Duration(span.Duration) * time.Microsecond,
			Status:       status,
			Tags:         span.Tags,
		}
		if s.queueSpan(sp) {
			queued++
		} else {
			dropped++
		}
		atomic.AddInt64(&s.stats.SpansReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

// ============================================================
// Loki Support
// ============================================================

func (s *Server) handleLoki(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestLoki(body, w)
}

// ingestLoki handles Grafana Loki push format
func (s *Server) ingestLoki(body []byte, w http.ResponseWriter) {
	// Loki push format
	var payload struct {
		Streams []struct {
			Stream map[string]string `json:"stream"` // Labels
			Values [][]string        `json:"values"` // [timestamp_ns, log_line]
		} `json:"streams"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Loki payload", http.StatusBadRequest)
		return
	}

	queued := 0
	dropped := 0
	for _, stream := range payload.Streams {
		// Extract service from labels
		service := stream.Stream["service"]
		if service == "" {
			service = stream.Stream["job"]
		}
		if service == "" {
			service = stream.Stream["app"]
		}
		if service == "" {
			service = "unknown"
		}

		// Extract level from labels
		level := stream.Stream["level"]
		if level == "" {
			level = stream.Stream["severity"]
		}
		if level == "" {
			level = "info"
		}

		for _, entry := range stream.Values {
			if len(entry) < 2 {
				continue
			}

			// Parse timestamp (nanoseconds as string)
			var ts time.Time
			if tsNano, err := parseInt64(entry[0]); err == nil {
				ts = time.Unix(0, tsNano)
			} else {
				ts = time.Now()
			}

			l := types.LogEntry{
				Timestamp: ts,
				Level:     level,
				Message:   entry[1],
				Service:   service,
				Tags:      stream.Stream,
			}
			if s.queueLog(l) {
				queued++
			} else {
				dropped++
			}
			atomic.AddInt64(&s.stats.LogsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "accepted",
		"queued":  queued,
		"dropped": dropped,
	})
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// ============================================================
// Splunk HEC Support
// ============================================================

func (s *Server) handleSplunkHEC(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestSplunkHEC(body, w)
}

func (s *Server) ingestSplunkHEC(body []byte, w http.ResponseWriter) {
	// Splunk HEC format - can be single event or batch
	var events []struct {
		Time       float64                `json:"time,omitempty"`
		Host       string                 `json:"host,omitempty"`
		Source     string                 `json:"source,omitempty"`
		Sourcetype string                 `json:"sourcetype,omitempty"`
		Index      string                 `json:"index,omitempty"`
		Event      interface{}            `json:"event"`
		Fields     map[string]interface{} `json:"fields,omitempty"`
	}

	// Try array first, then single event
	if err := json.Unmarshal(body, &events); err != nil {
		var single struct {
			Time       float64                `json:"time,omitempty"`
			Host       string                 `json:"host,omitempty"`
			Source     string                 `json:"source,omitempty"`
			Sourcetype string                 `json:"sourcetype,omitempty"`
			Index      string                 `json:"index,omitempty"`
			Event      interface{}            `json:"event"`
			Fields     map[string]interface{} `json:"fields,omitempty"`
		}
		if err := json.Unmarshal(body, &single); err != nil {
			http.Error(w, "Invalid Splunk HEC payload", http.StatusBadRequest)
			return
		}
		events = append(events, single)
	}

	queued := 0
	for _, event := range events {
		var message string
		switch e := event.Event.(type) {
		case string:
			message = e
		case map[string]interface{}:
			if msg, ok := e["message"].(string); ok {
				message = msg
			} else {
				b, _ := json.Marshal(e)
				message = string(b)
			}
		default:
			b, _ := json.Marshal(e)
			message = string(b)
		}

		l := types.LogEntry{
			Timestamp: time.Now(),
			Level:     "info",
			Message:   message,
			Service:   event.Source,
			Tags:      map[string]string{"host": event.Host, "sourcetype": event.Sourcetype},
		}
		if event.Time > 0 {
			l.Timestamp = time.Unix(int64(event.Time), 0)
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"text": "Success", "code": 0})
}

// ============================================================
// Elasticsearch Support
// ============================================================

func (s *Server) handleElasticsearch(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestElasticsearch(body, w)
}

func (s *Server) ingestElasticsearch(body []byte, w http.ResponseWriter) {
	// Elasticsearch bulk format: alternating action/doc lines
	lines := strings.Split(string(body), "\n")
	queued := 0

	for i := 0; i < len(lines)-1; i += 2 {
		// Skip action line, parse doc
		if i+1 >= len(lines) {
			break
		}
		docLine := lines[i+1]
		if docLine == "" {
			continue
		}

		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(docLine), &doc); err != nil {
			continue
		}

		message := ""
		if msg, ok := doc["message"].(string); ok {
			message = msg
		} else if msg, ok := doc["log"].(string); ok {
			message = msg
		} else {
			b, _ := json.Marshal(doc)
			message = string(b)
		}

		service := "elasticsearch"
		if svc, ok := doc["service"].(string); ok {
			service = svc
		}

		l := types.LogEntry{
			Timestamp: time.Now(),
			Level:     getString(doc, "level", "info"),
			Message:   message,
			Service:   service,
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"took": 1, "errors": false, "items": []interface{}{}})
}

// ============================================================
// InfluxDB Support
// ============================================================

func (s *Server) handleInfluxDB(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestInfluxDB(body, w, r)
}

func (s *Server) ingestInfluxDB(body []byte, w http.ResponseWriter, r *http.Request) {
	// InfluxDB line protocol: measurement,tag=value field=value timestamp
	lines := strings.Split(string(body), "\n")
	queued := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse line protocol: measurement,tags fields timestamp
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}

		// Parse measurement and tags
		measurementParts := strings.SplitN(parts[0], ",", 2)
		measurement := measurementParts[0]

		tags := make(map[string]string)
		if len(measurementParts) > 1 {
			for _, tag := range strings.Split(measurementParts[1], ",") {
				kv := strings.SplitN(tag, "=", 2)
				if len(kv) == 2 {
					tags[kv[0]] = kv[1]
				}
			}
		}

		// Parse fields
		fields := make(map[string]float64)
		for _, field := range strings.Split(parts[1], ",") {
			kv := strings.SplitN(field, "=", 2)
			if len(kv) == 2 {
				// Remove type suffix (i, u, etc.)
				valStr := strings.TrimRight(kv[1], "iu")
				if val, err := strconv.ParseFloat(valStr, 64); err == nil {
					fields[kv[0]] = val
				}
			}
		}

		// Create metrics for each field
		for fieldName, value := range fields {
			m := types.Metric{
				Name:      measurement + "_" + fieldName,
				Value:     value,
				Timestamp: time.Now(),
				Service:   tags["host"],
				Tags:      tags,
			}
			if s.queueMetric(m) {
				queued++
			}
			atomic.AddInt64(&s.stats.MetricsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusNoContent)
}

// ============================================================
// Graphite Support
// ============================================================

func (s *Server) handleGraphite(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestGraphite(body, w)
}

func (s *Server) ingestGraphite(body []byte, w http.ResponseWriter) {
	// Graphite plaintext: metric.path value timestamp
	lines := strings.Split(string(body), "\n")
	queued := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		ts := time.Now()
		if len(parts) >= 3 {
			if epoch, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
				ts = time.Unix(epoch, 0)
			}
		}

		m := types.Metric{
			Name:      parts[0],
			Value:     value,
			Timestamp: ts,
			Service:   "graphite",
		}
		if s.queueMetric(m) {
			queued++
		}
		atomic.AddInt64(&s.stats.MetricsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "accepted", "queued": queued})
}

// ============================================================
// StatsD Support (HTTP)
// ============================================================

func (s *Server) handleStatsD(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestStatsD(body, w)
}

func (s *Server) ingestStatsD(body []byte, w http.ResponseWriter) {
	// StatsD format: metric:value|type|@sample_rate|#tags
	lines := strings.Split(string(body), "\n")
	queued := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse metric:value|type
		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			continue
		}

		metricParts := strings.SplitN(parts[0], ":", 2)
		if len(metricParts) < 2 {
			continue
		}

		value, err := strconv.ParseFloat(metricParts[1], 64)
		if err != nil {
			continue
		}

		// Parse tags if present
		tags := make(map[string]string)
		for _, part := range parts[2:] {
			if strings.HasPrefix(part, "#") {
				tagStr := strings.TrimPrefix(part, "#")
				for _, tag := range strings.Split(tagStr, ",") {
					kv := strings.SplitN(tag, ":", 2)
					if len(kv) == 2 {
						tags[kv[0]] = kv[1]
					}
				}
			}
		}

		m := types.Metric{
			Name:      metricParts[0],
			Value:     value,
			Timestamp: time.Now(),
			Service:   tags["service"],
			Tags:      tags,
		}
		if s.queueMetric(m) {
			queued++
		}
		atomic.AddInt64(&s.stats.MetricsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "accepted", "queued": queued})
}

// ============================================================
// Fluentd/Fluent Bit Support
// ============================================================

func (s *Server) handleFluentd(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestFluentd(body, w)
}

func (s *Server) ingestFluentd(body []byte, w http.ResponseWriter) {
	// Fluentd JSON format: [tag, timestamp, record] or {tag, records: [...]}
	queued := 0

	// Try array format first
	var records []interface{}
	if err := json.Unmarshal(body, &records); err == nil {
		for _, record := range records {
			if rec, ok := record.(map[string]interface{}); ok {
				message := ""
				if msg, ok := rec["message"].(string); ok {
					message = msg
				} else if msg, ok := rec["log"].(string); ok {
					message = msg
				}

				l := types.LogEntry{
					Timestamp: time.Now(),
					Level:     getString(rec, "level", "info"),
					Message:   message,
					Service:   getString(rec, "tag", "fluentd"),
				}
				if s.queueLog(l) {
					queued++
				}
				atomic.AddInt64(&s.stats.LogsReceived, 1)
			}
		}
	} else {
		// Try object format
		var obj map[string]interface{}
		if err := json.Unmarshal(body, &obj); err == nil {
			message := ""
			if msg, ok := obj["message"].(string); ok {
				message = msg
			} else if msg, ok := obj["log"].(string); ok {
				message = msg
			}

			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     getString(obj, "level", "info"),
				Message:   message,
				Service:   getString(obj, "tag", "fluentd"),
			}
			if s.queueLog(l) {
				queued++
			}
			atomic.AddInt64(&s.stats.LogsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "accepted", "queued": queued})
}

// ============================================================
// Syslog Support (HTTP)
// ============================================================

func (s *Server) handleSyslog(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestSyslog(body, w)
}

func (s *Server) ingestSyslog(body []byte, w http.ResponseWriter) {
	// Syslog messages (one per line or JSON)
	lines := strings.Split(string(body), "\n")
	queued := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Try JSON first
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err == nil {
			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     getString(obj, "severity", "info"),
				Message:   getString(obj, "message", line),
				Service:   getString(obj, "hostname", "syslog"),
			}
			if s.queueLog(l) {
				queued++
			}
		} else {
			// Raw syslog line
			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     "info",
				Message:   line,
				Service:   "syslog",
			}
			if s.queueLog(l) {
				queued++
			}
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "accepted", "queued": queued})
}

// ============================================================
// New Relic Support
// ============================================================

func (s *Server) handleNewRelicLogs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestNewRelicLogs(body, w)
}

func (s *Server) ingestNewRelicLogs(body []byte, w http.ResponseWriter) {
	var logs []struct {
		Message   string                 `json:"message"`
		Timestamp int64                  `json:"timestamp"`
		Attrs     map[string]interface{} `json:"attributes"`
	}

	if err := json.Unmarshal(body, &logs); err != nil {
		http.Error(w, "Invalid New Relic logs payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, log := range logs {
		l := types.LogEntry{
			Timestamp: time.UnixMilli(log.Timestamp),
			Level:     "info",
			Message:   log.Message,
			Service:   "newrelic",
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"requestId": "accepted"})
}

func (s *Server) handleNewRelicMetrics(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestNewRelicMetrics(body, w)
}

func (s *Server) ingestNewRelicMetrics(body []byte, w http.ResponseWriter) {
	var payload []struct {
		Metrics []struct {
			Name       string                 `json:"name"`
			Type       string                 `json:"type"`
			Value      float64                `json:"value"`
			Timestamp  int64                  `json:"timestamp"`
			Attributes map[string]interface{} `json:"attributes"`
		} `json:"metrics"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid New Relic metrics payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, p := range payload {
		for _, metric := range p.Metrics {
			m := types.Metric{
				Name:      metric.Name,
				Value:     metric.Value,
				Timestamp: time.UnixMilli(metric.Timestamp),
				Service:   "newrelic",
			}
			if s.queueMetric(m) {
				queued++
			}
			atomic.AddInt64(&s.stats.MetricsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"requestId": "accepted"})
}

func (s *Server) handleNewRelicTraces(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestNewRelicTraces(body, w)
}

func (s *Server) ingestNewRelicTraces(body []byte, w http.ResponseWriter) {
	var payload []struct {
		Spans []struct {
			TraceID   string                 `json:"trace.id"`
			ID        string                 `json:"id"`
			Name      string                 `json:"name"`
			Timestamp int64                  `json:"timestamp"`
			Duration  float64                `json:"duration.ms"`
			Attrs     map[string]interface{} `json:"attributes"`
		} `json:"spans"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid New Relic traces payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, p := range payload {
		for _, span := range p.Spans {
			sp := types.Span{
				TraceID:   span.TraceID,
				SpanID:    span.ID,
				Service:   "newrelic",
				Operation: span.Name,
				StartTime: time.UnixMilli(span.Timestamp),
				Duration:  time.Duration(span.Duration) * time.Millisecond,
				Status:    "OK",
			}
			if s.queueSpan(sp) {
				queued++
			}
			atomic.AddInt64(&s.stats.SpansReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"requestId": "accepted"})
}

// ============================================================
// Honeycomb Support
// ============================================================

func (s *Server) handleHoneycomb(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestHoneycomb(body, w)
}

func (s *Server) ingestHoneycomb(body []byte, w http.ResponseWriter) {
	// Honeycomb event format
	var events []map[string]interface{}
	if err := json.Unmarshal(body, &events); err != nil {
		// Try single event
		var single map[string]interface{}
		if err := json.Unmarshal(body, &single); err != nil {
			http.Error(w, "Invalid Honeycomb payload", http.StatusBadRequest)
			return
		}
		events = []map[string]interface{}{single}
	}

	queued := 0
	for _, event := range events {
		// Check if it's a span (has trace.trace_id)
		if traceID, ok := event["trace.trace_id"].(string); ok {
			sp := types.Span{
				TraceID:   traceID,
				SpanID:    getString(event, "trace.span_id", ""),
				Service:   getString(event, "service_name", "honeycomb"),
				Operation: getString(event, "name", ""),
				StartTime: time.Now(),
				Status:    "OK",
			}
			if s.queueSpan(sp) {
				queued++
			}
			atomic.AddInt64(&s.stats.SpansReceived, 1)
		} else {
			// Treat as log
			message := ""
			if msg, ok := event["message"].(string); ok {
				message = msg
			} else {
				b, _ := json.Marshal(event)
				message = string(b)
			}

			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     getString(event, "level", "info"),
				Message:   message,
				Service:   getString(event, "service_name", "honeycomb"),
			}
			if s.queueLog(l) {
				queued++
			}
			atomic.AddInt64(&s.stats.LogsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": queued})
}

// ============================================================
// AWS X-Ray Support
// ============================================================

func (s *Server) handleAWSXRay(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestAWSXRay(body, w)
}

func (s *Server) ingestAWSXRay(body []byte, w http.ResponseWriter) {
	var payload struct {
		TraceSegmentDocuments []string `json:"TraceSegmentDocuments"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid X-Ray payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, doc := range payload.TraceSegmentDocuments {
		var segment struct {
			TraceID   string  `json:"trace_id"`
			ID        string  `json:"id"`
			Name      string  `json:"name"`
			StartTime float64 `json:"start_time"`
			EndTime   float64 `json:"end_time"`
			Fault     bool    `json:"fault"`
			Error     bool    `json:"error"`
		}
		if err := json.Unmarshal([]byte(doc), &segment); err != nil {
			continue
		}

		status := "OK"
		if segment.Fault || segment.Error {
			status = "ERROR"
		}

		sp := types.Span{
			TraceID:   segment.TraceID,
			SpanID:    segment.ID,
			Service:   segment.Name,
			Operation: segment.Name,
			StartTime: time.Unix(int64(segment.StartTime), 0),
			Duration:  time.Duration((segment.EndTime - segment.StartTime) * float64(time.Second)),
			Status:    status,
		}
		if s.queueSpan(sp) {
			queued++
		}
		atomic.AddInt64(&s.stats.SpansReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"UnprocessedTraceSegments": []interface{}{}})
}

// ============================================================
// CloudWatch Support
// ============================================================

func (s *Server) handleCloudWatch(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestCloudWatch(body, w)
}

func (s *Server) ingestCloudWatch(body []byte, w http.ResponseWriter) {
	// CloudWatch Logs format
	var payload struct {
		LogEvents []struct {
			Timestamp int64  `json:"timestamp"`
			Message   string `json:"message"`
		} `json:"logEvents"`
		LogGroup  string `json:"logGroupName"`
		LogStream string `json:"logStreamName"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid CloudWatch payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, event := range payload.LogEvents {
		l := types.LogEntry{
			Timestamp: time.UnixMilli(event.Timestamp),
			Level:     "info",
			Message:   event.Message,
			Service:   payload.LogGroup,
			Tags:      map[string]string{"logStream": payload.LogStream},
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"nextSequenceToken": "accepted"})
}

// ============================================================
// Google Cloud Trace Support
// ============================================================

func (s *Server) handleGoogleCloudTrace(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestGoogleCloudTrace(body, w)
}

func (s *Server) ingestGoogleCloudTrace(body []byte, w http.ResponseWriter) {
	var payload struct {
		Spans []struct {
			SpanID      string `json:"spanId"`
			DisplayName struct {
				Value string `json:"value"`
			} `json:"displayName"`
			StartTime string `json:"startTime"`
			EndTime   string `json:"endTime"`
		} `json:"spans"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Google Cloud Trace payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, span := range payload.Spans {
		startTime, _ := time.Parse(time.RFC3339Nano, span.StartTime)
		endTime, _ := time.Parse(time.RFC3339Nano, span.EndTime)

		sp := types.Span{
			SpanID:    span.SpanID,
			Service:   "gcp",
			Operation: span.DisplayName.Value,
			StartTime: startTime,
			Duration:  endTime.Sub(startTime),
			Status:    "OK",
		}
		if s.queueSpan(sp) {
			queued++
		}
		atomic.AddInt64(&s.stats.SpansReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{})
}

// ============================================================
// Google Cloud Logging Support
// ============================================================

func (s *Server) handleGoogleCloudLogging(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestGoogleCloudLogging(body, w)
}

func (s *Server) ingestGoogleCloudLogging(body []byte, w http.ResponseWriter) {
	var payload struct {
		Entries []struct {
			LogName   string `json:"logName"`
			Severity  string `json:"severity"`
			Timestamp string `json:"timestamp"`
			TextPayload string `json:"textPayload"`
			JSONPayload map[string]interface{} `json:"jsonPayload"`
		} `json:"entries"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Google Cloud Logging payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, entry := range payload.Entries {
		message := entry.TextPayload
		if message == "" && entry.JSONPayload != nil {
			b, _ := json.Marshal(entry.JSONPayload)
			message = string(b)
		}

		ts, _ := time.Parse(time.RFC3339Nano, entry.Timestamp)

		l := types.LogEntry{
			Timestamp: ts,
			Level:     strings.ToLower(entry.Severity),
			Message:   message,
			Service:   entry.LogName,
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{})
}

// ============================================================
// Azure Monitor Support
// ============================================================

func (s *Server) handleAzureMonitor(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestAzureMonitor(body, w)
}

func (s *Server) ingestAzureMonitor(body []byte, w http.ResponseWriter) {
	// Azure Monitor custom metrics/logs format
	var payload struct {
		Records []struct {
			Time       string                 `json:"time"`
			Category   string                 `json:"category"`
			Level      string                 `json:"level"`
			Properties map[string]interface{} `json:"properties"`
		} `json:"records"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid Azure Monitor payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, record := range payload.Records {
		ts, _ := time.Parse(time.RFC3339Nano, record.Time)
		message, _ := json.Marshal(record.Properties)

		l := types.LogEntry{
			Timestamp: ts,
			Level:     strings.ToLower(record.Level),
			Message:   string(message),
			Service:   record.Category,
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"itemsReceived": queued})
}

// ============================================================
// Dynatrace Support
// ============================================================

func (s *Server) handleDynatrace(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	if strings.Contains(r.URL.Path, "metrics") {
		s.ingestDynatraceMetrics(body, w)
	} else {
		s.ingestDynatraceLogs(body, w)
	}
}

func (s *Server) ingestDynatraceMetrics(body []byte, w http.ResponseWriter) {
	// Dynatrace metrics line format
	lines := strings.Split(string(body), "\n")
	queued := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse: metric_name,dims value
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		nameParts := strings.SplitN(parts[0], ",", 2)
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		m := types.Metric{
			Name:      nameParts[0],
			Value:     value,
			Timestamp: time.Now(),
			Service:   "dynatrace",
		}
		if s.queueMetric(m) {
			queued++
		}
		atomic.AddInt64(&s.stats.MetricsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"linesOk": queued})
}

func (s *Server) ingestDynatraceLogs(body []byte, w http.ResponseWriter) {
	var logs []struct {
		Content   string `json:"content"`
		Timestamp string `json:"timestamp"`
		Severity  string `json:"severity"`
	}

	if err := json.Unmarshal(body, &logs); err != nil {
		http.Error(w, "Invalid Dynatrace logs payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, log := range logs {
		ts, _ := time.Parse(time.RFC3339Nano, log.Timestamp)

		l := types.LogEntry{
			Timestamp: ts,
			Level:     strings.ToLower(log.Severity),
			Message:   log.Content,
			Service:   "dynatrace",
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": queued})
}

// ============================================================
// AppDynamics Support
// ============================================================

func (s *Server) handleAppDynamics(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestAppDynamics(body, w)
}

func (s *Server) ingestAppDynamics(body []byte, w http.ResponseWriter) {
	var events []struct {
		EventType string                 `json:"eventType"`
		Timestamp int64                  `json:"eventTime"`
		Summary   string                 `json:"summary"`
		Details   map[string]interface{} `json:"details"`
	}

	if err := json.Unmarshal(body, &events); err != nil {
		http.Error(w, "Invalid AppDynamics payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, event := range events {
		l := types.LogEntry{
			Timestamp: time.UnixMilli(event.Timestamp),
			Level:     "info",
			Message:   event.Summary,
			Service:   "appdynamics",
			Tags:      map[string]string{"eventType": event.EventType},
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"received": queued})
}

// ============================================================
// SignalFx Support
// ============================================================

func (s *Server) handleSignalFx(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestSignalFx(body, w)
}

func (s *Server) ingestSignalFx(body []byte, w http.ResponseWriter) {
	var payload struct {
		Gauge   []signalFxDatapoint `json:"gauge"`
		Counter []signalFxDatapoint `json:"counter"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid SignalFx payload", http.StatusBadRequest)
		return
	}

	queued := 0
	for _, dp := range append(payload.Gauge, payload.Counter...) {
		m := types.Metric{
			Name:      dp.Metric,
			Value:     dp.Value,
			Timestamp: time.UnixMilli(dp.Timestamp),
			Service:   "signalfx",
		}
		if s.queueMetric(m) {
			queued++
		}
		atomic.AddInt64(&s.stats.MetricsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type signalFxDatapoint struct {
	Metric     string            `json:"metric"`
	Value      float64           `json:"value"`
	Timestamp  int64             `json:"timestamp"`
	Dimensions map[string]string `json:"dimensions"`
}

// ============================================================
// Logstash Support
// ============================================================

func (s *Server) handleLogstash(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestLogstash(body, w)
}

func (s *Server) ingestLogstash(body []byte, w http.ResponseWriter) {
	// Logstash JSON format
	var events []map[string]interface{}
	if err := json.Unmarshal(body, &events); err != nil {
		// Try single event
		var single map[string]interface{}
		if err := json.Unmarshal(body, &single); err != nil {
			http.Error(w, "Invalid Logstash payload", http.StatusBadRequest)
			return
		}
		events = []map[string]interface{}{single}
	}

	queued := 0
	for _, event := range events {
		message := getString(event, "message", "")
		if message == "" {
			b, _ := json.Marshal(event)
			message = string(b)
		}

		l := types.LogEntry{
			Timestamp: time.Now(),
			Level:     getString(event, "level", "info"),
			Message:   message,
			Service:   getString(event, "host", "logstash"),
		}
		if s.queueLog(l) {
			queued++
		}
		atomic.AddInt64(&s.stats.LogsReceived, 1)
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "count": queued})
}

// ============================================================
// Sumo Logic Support
// ============================================================

func (s *Server) handleSumoLogic(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.ingestSumoLogic(body, w)
}

func (s *Server) ingestSumoLogic(body []byte, w http.ResponseWriter) {
	// Sumo Logic HTTP source - can be JSON or raw text
	queued := 0

	// Try JSON array first
	var events []map[string]interface{}
	if err := json.Unmarshal(body, &events); err == nil {
		for _, event := range events {
			message := getString(event, "message", "")
			if message == "" {
				b, _ := json.Marshal(event)
				message = string(b)
			}

			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     getString(event, "level", "info"),
				Message:   message,
				Service:   getString(event, "_source", "sumologic"),
			}
			if s.queueLog(l) {
				queued++
			}
			atomic.AddInt64(&s.stats.LogsReceived, 1)
		}
	} else {
		// Raw text, split by lines
		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			l := types.LogEntry{
				Timestamp: time.Now(),
				Level:     "info",
				Message:   line,
				Service:   "sumologic",
			}
			if s.queueLog(l) {
				queued++
			}
			atomic.AddInt64(&s.stats.LogsReceived, 1)
		}
	}

	s.stats.LastIngest = time.Now()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
}
