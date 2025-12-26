package context

import (
	"time"

	"github.com/reductrai/agent/internal/infra"
)

// EnrichedContext contains privacy-preserving context for LLM analysis
// Target size: 10-20KB (configurable via MaxLogs, MaxTraces in Extractor)
type EnrichedContext struct {
	// Anomaly that triggered this context extraction
	Anomaly AnomalyInfo `json:"anomaly"`

	// Scope analysis: isolated vs cascading (THE KEY INSIGHT)
	Scope ScopeAnalysis `json:"scope"`

	// Service metrics with baselines
	ServiceMetrics ServiceMetrics `json:"serviceMetrics"`

	// Error breakdown by type (percentages, not raw counts)
	ErrorBreakdown *ErrorBreakdown `json:"errorBreakdown,omitempty"`

	// Correlated services also experiencing issues
	Correlations []ServiceCorrelation `json:"correlations,omitempty"`

	// Sample error logs (redacted)
	ErrorLogs []RedactedLog `json:"errorLogs,omitempty"`

	// Sample failing traces (redacted)
	FailingTraces []RedactedSpan `json:"failingTraces,omitempty"`

	// Affected endpoints (paths only)
	AffectedEndpoints []EndpointStats `json:"affectedEndpoints,omitempty"`

	// Time analysis
	TimeAnalysis *TimeAnalysis `json:"timeAnalysis,omitempty"`

	// Historical pattern matching (THE SECRET SAUCE)
	Fingerprint IncidentFingerprint `json:"fingerprint"`

	// Current infrastructure state (deployment info, replicas, resources, etc.)
	InfraState *infra.InfraState `json:"infraState,omitempty"`

	// Binary version for compatibility checks
	BinaryVersion string `json:"binaryVersion"`

	// Timestamp when context was extracted
	ExtractedAt int64 `json:"extractedAt"`
}

// AnomalyInfo represents the anomaly that triggered context extraction
type AnomalyInfo struct {
	Service    string  `json:"service"`
	Type       string  `json:"type"`       // error_spike, latency_spike, traffic_drop
	Severity   string  `json:"severity"`   // low, medium, high, critical
	Value      float64 `json:"value"`      // Actual value that exceeded threshold
	Threshold  float64 `json:"threshold"`  // Threshold that was exceeded
	DetectedAt int64   `json:"detectedAt"` // Unix timestamp
}

// ServiceMetrics contains current metrics with baselines for comparison
type ServiceMetrics struct {
	ErrorRate          float64 `json:"errorRate"`          // Current error rate (0-1)
	ErrorRateBaseline  float64 `json:"errorRateBaseline"`  // Baseline error rate
	LatencyP99         float64 `json:"latencyP99"`         // Current P99 latency in ms
	LatencyBaseline    float64 `json:"latencyBaseline"`    // Baseline P99 latency
	Throughput         float64 `json:"throughput"`         // Current requests/sec
	ThroughputBaseline float64 `json:"throughputBaseline"` // Baseline throughput
}

// ErrorBreakdown categorizes errors by type (percentages)
type ErrorBreakdown struct {
	Timeout         float64 `json:"timeout,omitempty"`         // % of errors that are timeouts
	ConnectionError float64 `json:"connectionError,omitempty"` // % connection refused/reset
	ServerError5xx  float64 `json:"serverError5xx,omitempty"`  // % 5xx responses
	ClientError4xx  float64 `json:"clientError4xx,omitempty"`  // % 4xx responses
	Other           float64 `json:"other,omitempty"`           // % other errors
}

// ServiceCorrelation represents a related service also experiencing issues
type ServiceCorrelation struct {
	Service          string  `json:"service"`
	Direction        string  `json:"direction"` // upstream, downstream
	CorrelationScore float64 `json:"correlationScore"`
	ErrorRate        float64 `json:"errorRate"`
	Latency          float64 `json:"latency"`
}

// RedactedLog is a log entry with PII removed
type RedactedLog struct {
	Timestamp   int64  `json:"timestamp"`             // Unix timestamp
	Level       string `json:"level"`                 // ERROR, WARN, etc.
	MessageHash string `json:"messageHash"`           // For deduplication
	Template    string `json:"template"`              // Redacted message template
	Count       int    `json:"count,omitempty"`       // If grouped by template
	ErrorType   string `json:"errorType,omitempty"`   // Extracted error type
}

// RedactedSpan is a span with PII removed
type RedactedSpan struct {
	Operation  string  `json:"operation"`            // e.g., GET /users/[ID]
	DurationMs float64 `json:"durationMs"`           // Duration in milliseconds
	Status     string  `json:"status"`               // OK or ERROR
	ErrorType  string  `json:"errorType,omitempty"`  // Type of error if status=ERROR
	Count      int     `json:"count,omitempty"`      // If grouped
}

// EndpointStats contains aggregated stats for an endpoint
type EndpointStats struct {
	Path       string  `json:"path"`       // Redacted path: /users/[ID]/orders
	ErrorRate  float64 `json:"errorRate"`  // Error rate for this endpoint
	LatencyP99 float64 `json:"latencyP99"` // P99 latency in ms
	Count      int     `json:"count"`      // Request count
}

// TimeAnalysis provides temporal context about the issue
type TimeAnalysis struct {
	IssueStartedAt int64   `json:"issueStartedAt"`          // When issue began (Unix)
	DurationMs     int64   `json:"durationMs"`              // How long it's been ongoing
	Trend          string  `json:"trend"`                   // improving, stable, worsening
	PeakErrorRate  float64 `json:"peakErrorRate,omitempty"` // Highest error rate seen
}

// ScopeAnalysis determines if the anomaly is isolated or part of a cascade
// This is critical for LLM reasoning - "isolated" means fix this service,
// "upstream" means the root cause is likely elsewhere
type ScopeAnalysis struct {
	Scope       string   `json:"scope"`       // isolated | cascading | upstream | downstream
	Reasoning   string   `json:"reasoning"`   // Human-readable explanation
	HealthyDeps []string `json:"healthyDeps"` // Dependencies that are healthy
	FailingDeps []string `json:"failingDeps"` // Dependencies also failing
}

// IncidentFingerprint enables historical pattern matching
// Hash is generated from (service + anomaly_type + sorted_correlated_services)
type IncidentFingerprint struct {
	Hash           string            `json:"hash"`                     // First 16 hex chars of SHA256
	MatchedHistory []HistoricalMatch `json:"matchedHistory,omitempty"` // Past incidents with same fingerprint
	Confidence     float64           `json:"confidence"`               // 0.0-1.0 based on historical success rate
}

// HistoricalMatch represents a past incident with the same fingerprint
type HistoricalMatch struct {
	Timestamp     int64  `json:"timestamp"`     // Unix timestamp when it occurred
	DurationSec   int64  `json:"durationSec"`   // How long the incident lasted
	AppliedFix    string `json:"appliedFix"`    // Command that was executed
	FixSucceeded  bool   `json:"fixSucceeded"`  // Did the fix resolve the issue?
	TimeToResolve int64  `json:"timeToResolve"` // Seconds from fix to resolution
}

// ExtractorConfig configures the context extraction limits
type ExtractorConfig struct {
	MaxLogs      int           // Max error logs to include (default: 20)
	MaxTraces    int           // Max failing traces to include (default: 30)
	MaxEndpoints int           // Max affected endpoints (default: 10)
	LookbackMin  int           // Minutes to look back (default: 15)
	Timeout      time.Duration // Query timeout (default: 10s)
}

// DefaultExtractorConfig returns sensible defaults
func DefaultExtractorConfig() ExtractorConfig {
	return ExtractorConfig{
		MaxLogs:      20,
		MaxTraces:    30,
		MaxEndpoints: 10,
		LookbackMin:  15,
		Timeout:      10 * time.Second,
	}
}
