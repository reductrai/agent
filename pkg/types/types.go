package types

import "time"

// Metric represents a single metric data point
type Metric struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
	Service   string            `json:"service"`
}

// LogEntry represents a log line
type LogEntry struct {
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Service   string            `json:"service"`
	Tags      map[string]string `json:"tags"`
}

// Span represents a trace span
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	Service      string            `json:"service"`
	Operation    string            `json:"operation"`
	StartTime    time.Time         `json:"start_time"`
	Duration     time.Duration     `json:"duration"`
	Status       string            `json:"status"`
	Tags         map[string]string `json:"tags"`
}

// ServiceHealth represents aggregated health for a service
type ServiceHealth struct {
	Service       string   `json:"service"`
	ErrorRate     float64  `json:"error_rate"`
	LatencyP50Ms  float64  `json:"latency_p50_ms"`
	LatencyP99Ms  float64  `json:"latency_p99_ms"`
	RequestsPerMin float64 `json:"requests_per_min"`
	TopErrors     []string `json:"top_errors,omitempty"`
	Dependencies  []string `json:"dependencies,omitempty"`
}

// Anomaly represents a detected anomaly
type Anomaly struct {
	Service   string  `json:"service"`
	Type      string  `json:"type"` // error_spike, latency_spike, traffic_drop
	Severity  string  `json:"severity"` // low, medium, high, critical
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	DetectedAt time.Time `json:"detected_at"`
}
