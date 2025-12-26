package context

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/reductrai/agent/internal/infra"
	"github.com/reductrai/agent/pkg/types"
)

// Version is the binary version (set at build time)
var Version = "0.2.0"

// Extractor extracts enriched context from DuckDB for LLM analysis
type Extractor struct {
	db                   *sql.DB
	redactor             *PIIRedactor
	config               ExtractorConfig
	infraCollector       *infra.Collector
	scopeAnalyzer        *ScopeAnalyzer
	fingerprintGenerator *FingerprintGenerator
}

// Queryable interface for DuckDB access
type Queryable interface {
	Query(sql string) (*sql.Rows, error)
}

// NewExtractor creates a new context extractor
func NewExtractor(db *sql.DB) *Extractor {
	return &Extractor{
		db:                   db,
		redactor:             NewPIIRedactor(),
		config:               DefaultExtractorConfig(),
		infraCollector:       infra.NewCollector(),
		scopeAnalyzer:        NewScopeAnalyzer(db),
		fingerprintGenerator: NewFingerprintGenerator(db),
	}
}

// NewExtractorWithConfig creates an extractor with custom config
func NewExtractorWithConfig(db *sql.DB, config ExtractorConfig) *Extractor {
	return &Extractor{
		db:                   db,
		redactor:             NewPIIRedactor(),
		config:               config,
		infraCollector:       infra.NewCollector(),
		scopeAnalyzer:        NewScopeAnalyzer(db),
		fingerprintGenerator: NewFingerprintGenerator(db),
	}
}

// Extract extracts enriched context for an anomaly
func (e *Extractor) Extract(anomaly types.Anomaly) (*EnrichedContext, error) {
	now := time.Now()
	lookback := time.Duration(e.config.LookbackMin) * time.Minute
	since := now.Add(-lookback)

	ctx := &EnrichedContext{
		Anomaly: AnomalyInfo{
			Service:    anomaly.Service,
			Type:       anomaly.Type,
			Severity:   anomaly.Severity,
			Value:      anomaly.Value,
			Threshold:  anomaly.Threshold,
			DetectedAt: anomaly.DetectedAt.Unix(),
		},
		BinaryVersion: Version,
		ExtractedAt:   now.Unix(),
	}

	// 1. Get service metrics with baselines
	ctx.ServiceMetrics = e.extractServiceMetrics(anomaly.Service, since)

	// 2. Calculate error breakdown
	ctx.ErrorBreakdown = e.extractErrorBreakdown(anomaly.Service, since)

	// 3. Find correlated services
	ctx.Correlations = e.extractCorrelations(anomaly.Service, since)

	// 4. Get sample error logs (redacted)
	ctx.ErrorLogs = e.extractErrorLogs(anomaly.Service, since)

	// 5. Get sample failing traces (redacted)
	ctx.FailingTraces = e.extractFailingTraces(anomaly.Service, since)

	// 6. Get affected endpoints
	ctx.AffectedEndpoints = e.extractAffectedEndpoints(anomaly.Service, since)

	// 7. Calculate time analysis
	ctx.TimeAnalysis = e.extractTimeAnalysis(anomaly.Service, anomaly.DetectedAt)

	// 8. Collect infrastructure state (deployment info, replicas, resources)
	ctx.InfraState = e.extractInfraState(anomaly.Service)

	// 9. Analyze scope: isolated vs cascading vs upstream vs downstream
	ctx.Scope = e.scopeAnalyzer.AnalyzeWithMetrics(
		anomaly.Service,
		ctx.Correlations,
		ctx.ServiceMetrics,
	)

	// 10. Generate fingerprint for historical pattern matching
	ctx.Fingerprint = e.fingerprintGenerator.Generate(
		anomaly.Service,
		anomaly.Type,
		ctx.Correlations,
	)

	return ctx, nil
}

// extractInfraState collects current infrastructure state for the service
func (e *Extractor) extractInfraState(serviceName string) *infra.InfraState {
	if e.infraCollector == nil {
		return nil
	}

	// Use a timeout context for infra collection
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	// Try to detect namespace from service name or use default
	namespace := "default"
	// Common patterns: service-name-prod, prod-service-name
	if strings.Contains(serviceName, "-prod") || strings.Contains(serviceName, "prod-") {
		namespace = "production"
	} else if strings.Contains(serviceName, "-staging") || strings.Contains(serviceName, "staging-") {
		namespace = "staging"
	}

	state, err := e.infraCollector.Collect(ctx, serviceName, namespace)
	if err != nil {
		return nil
	}

	// Also check for database/cache state if the service name suggests it
	if state != nil {
		if dbState := e.infraCollector.CollectDatabase(ctx, serviceName); dbState != nil {
			state.Database = dbState
		}
		if cacheState := e.infraCollector.CollectCache(ctx, serviceName); cacheState != nil {
			state.Cache = cacheState
		}
	}

	return state
}

// extractServiceMetrics gets current metrics and baselines for a service
func (e *Extractor) extractServiceMetrics(service string, since time.Time) ServiceMetrics {
	metrics := ServiceMetrics{}

	// Get current metrics (last 5 minutes)
	recentSince := time.Now().Add(-5 * time.Minute)
	query := `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(status) = 'ERROR' THEN 1 ELSE 0 END) as errors,
			COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_us), 0) as latency_p99
		FROM spans
		WHERE service = ? AND timestamp > ?
	`
	row := e.db.QueryRow(query, service, recentSince)

	var total, errors int64
	var latencyP99 float64
	if err := row.Scan(&total, &errors, &latencyP99); err == nil && total > 0 {
		metrics.ErrorRate = float64(errors) / float64(total)
		metrics.LatencyP99 = latencyP99 / 1000 // us to ms
		metrics.Throughput = float64(total) / 5 // per minute
	}

	// Get baseline metrics (15-60 minutes ago for comparison)
	baselineSince := time.Now().Add(-60 * time.Minute)
	baselineUntil := time.Now().Add(-15 * time.Minute)
	baselineQuery := `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(status) = 'ERROR' THEN 1 ELSE 0 END) as errors,
			COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_us), 0) as latency_p99
		FROM spans
		WHERE service = ? AND timestamp > ? AND timestamp < ?
	`
	row = e.db.QueryRow(baselineQuery, service, baselineSince, baselineUntil)

	var baseTotal, baseErrors int64
	var baseLatency float64
	if err := row.Scan(&baseTotal, &baseErrors, &baseLatency); err == nil && baseTotal > 0 {
		metrics.ErrorRateBaseline = float64(baseErrors) / float64(baseTotal)
		metrics.LatencyBaseline = baseLatency / 1000
		metrics.ThroughputBaseline = float64(baseTotal) / 45 // per minute over 45 min window
	}

	return metrics
}

// extractErrorBreakdown categorizes errors by type
func (e *Extractor) extractErrorBreakdown(service string, since time.Time) *ErrorBreakdown {
	// Get all error logs and categorize them
	query := `
		SELECT message FROM logs
		WHERE service = ? AND UPPER(level) IN ('ERROR', 'WARN', 'FATAL')
		AND timestamp > ?
		LIMIT 100
	`

	rows, err := e.db.Query(query, service, since)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var timeout, connection, server5xx, client4xx, other int
	total := 0

	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			continue
		}
		total++

		errorType := ExtractErrorType(message)
		switch errorType {
		case "timeout":
			timeout++
		case "connection_error":
			connection++
		case "server_error":
			server5xx++
		case "validation_error":
			client4xx++
		default:
			other++
		}
	}

	if total == 0 {
		return nil
	}

	return &ErrorBreakdown{
		Timeout:         float64(timeout) / float64(total) * 100,
		ConnectionError: float64(connection) / float64(total) * 100,
		ServerError5xx:  float64(server5xx) / float64(total) * 100,
		ClientError4xx:  float64(client4xx) / float64(total) * 100,
		Other:           float64(other) / float64(total) * 100,
	}
}

// extractCorrelations finds services correlated with the issue
func (e *Extractor) extractCorrelations(service string, since time.Time) []ServiceCorrelation {
	correlations := []ServiceCorrelation{}

	// Find downstream services (that this service calls) with high error rates
	downstreamQuery := `
		SELECT
			child.service,
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(child.status) = 'ERROR' THEN 1 ELSE 0 END) as errors,
			AVG(child.duration_us) / 1000 as avg_latency
		FROM spans parent
		JOIN spans child ON parent.span_id = child.parent_span_id
		WHERE parent.service = ?
		  AND child.service != ?
		  AND parent.timestamp > ?
		GROUP BY child.service
		HAVING errors > 0
		ORDER BY CAST(errors AS FLOAT) / CAST(total AS FLOAT) DESC
		LIMIT 5
	`

	rows, err := e.db.Query(downstreamQuery, service, service, since)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var svc string
			var total, errors int64
			var latency float64
			if err := rows.Scan(&svc, &total, &errors, &latency); err == nil && total > 0 {
				errorRate := float64(errors) / float64(total)
				// High correlation if error rate > 5%
				if errorRate > 0.05 {
					correlations = append(correlations, ServiceCorrelation{
						Service:          svc,
						Direction:        "downstream",
						CorrelationScore: errorRate, // Using error rate as proxy for correlation
						ErrorRate:        errorRate,
						Latency:          latency,
					})
				}
			}
		}
	}

	// Find upstream services (that call this service) with high error rates
	upstreamQuery := `
		SELECT
			parent.service,
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(parent.status) = 'ERROR' THEN 1 ELSE 0 END) as errors,
			AVG(parent.duration_us) / 1000 as avg_latency
		FROM spans parent
		JOIN spans child ON parent.span_id = child.parent_span_id
		WHERE child.service = ?
		  AND parent.service != ?
		  AND parent.timestamp > ?
		GROUP BY parent.service
		HAVING errors > 0
		ORDER BY CAST(errors AS FLOAT) / CAST(total AS FLOAT) DESC
		LIMIT 5
	`

	rows2, err := e.db.Query(upstreamQuery, service, service, since)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var svc string
			var total, errors int64
			var latency float64
			if err := rows2.Scan(&svc, &total, &errors, &latency); err == nil && total > 0 {
				errorRate := float64(errors) / float64(total)
				if errorRate > 0.05 {
					correlations = append(correlations, ServiceCorrelation{
						Service:          svc,
						Direction:        "upstream",
						CorrelationScore: errorRate,
						ErrorRate:        errorRate,
						Latency:          latency,
					})
				}
			}
		}
	}

	return correlations
}

// extractErrorLogs gets sample error logs with PII redaction
func (e *Extractor) extractErrorLogs(service string, since time.Time) []RedactedLog {
	query := `
		SELECT timestamp, level, message FROM logs
		WHERE service = ? AND UPPER(level) IN ('ERROR', 'WARN', 'FATAL')
		AND timestamp > ?
		ORDER BY timestamp DESC
		LIMIT ?
	`

	rows, err := e.db.Query(query, service, since, e.config.MaxLogs*2) // Get more for grouping
	if err != nil {
		return nil
	}
	defer rows.Close()

	// Group by template
	templateCounts := make(map[string]*RedactedLog)
	templateOrder := []string{} // Track order

	for rows.Next() {
		var ts time.Time
		var level, message string
		if err := rows.Scan(&ts, &level, &message); err != nil {
			continue
		}

		// Redact and extract template
		template, hash := e.redactor.ExtractTemplate(message)
		errorType := ExtractErrorType(message)

		if existing, ok := templateCounts[hash]; ok {
			existing.Count++
		} else {
			templateCounts[hash] = &RedactedLog{
				Timestamp:   ts.Unix(),
				Level:       strings.ToUpper(level),
				MessageHash: hash,
				Template:    template,
				Count:       1,
				ErrorType:   errorType,
			}
			templateOrder = append(templateOrder, hash)
		}
	}

	// Convert to slice, limited to MaxLogs
	logs := make([]RedactedLog, 0, e.config.MaxLogs)
	for _, hash := range templateOrder {
		if len(logs) >= e.config.MaxLogs {
			break
		}
		logs = append(logs, *templateCounts[hash])
	}

	return logs
}

// extractFailingTraces gets sample failing spans with PII redaction
func (e *Extractor) extractFailingTraces(service string, since time.Time) []RedactedSpan {
	query := `
		SELECT
			operation,
			duration_us / 1000 as duration_ms,
			status,
			tags
		FROM spans
		WHERE service = ? AND UPPER(status) = 'ERROR'
		AND timestamp > ?
		ORDER BY timestamp DESC
		LIMIT ?
	`

	rows, err := e.db.Query(query, service, since, e.config.MaxTraces*2)
	if err != nil {
		return nil
	}
	defer rows.Close()

	// Group by operation (redacted)
	opCounts := make(map[string]*RedactedSpan)
	opOrder := []string{}

	for rows.Next() {
		var operation string
		var durationMs float64
		var status, tagsJSON string
		if err := rows.Scan(&operation, &durationMs, &status, &tagsJSON); err != nil {
			continue
		}

		// Redact the operation path
		redactedOp := e.redactor.RedactPath(operation)

		// Try to extract error type from tags
		errorType := ""
		if strings.Contains(tagsJSON, "error") || strings.Contains(tagsJSON, "exception") {
			errorType = "exception"
		} else if strings.Contains(tagsJSON, "timeout") {
			errorType = "timeout"
		}

		if existing, ok := opCounts[redactedOp]; ok {
			existing.Count++
			// Keep max duration
			if durationMs > existing.DurationMs {
				existing.DurationMs = durationMs
			}
		} else {
			opCounts[redactedOp] = &RedactedSpan{
				Operation:  redactedOp,
				DurationMs: durationMs,
				Status:     status,
				ErrorType:  errorType,
				Count:      1,
			}
			opOrder = append(opOrder, redactedOp)
		}
	}

	// Convert to slice
	spans := make([]RedactedSpan, 0, e.config.MaxTraces)
	for _, op := range opOrder {
		if len(spans) >= e.config.MaxTraces {
			break
		}
		spans = append(spans, *opCounts[op])
	}

	return spans
}

// extractAffectedEndpoints gets endpoints with high error rates
func (e *Extractor) extractAffectedEndpoints(service string, since time.Time) []EndpointStats {
	query := `
		SELECT
			operation,
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(status) = 'ERROR' THEN 1 ELSE 0 END) as errors,
			COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_us), 0) / 1000 as latency_p99
		FROM spans
		WHERE service = ? AND timestamp > ?
		GROUP BY operation
		HAVING errors > 0
		ORDER BY CAST(errors AS FLOAT) / CAST(total AS FLOAT) DESC
		LIMIT ?
	`

	rows, err := e.db.Query(query, service, since, e.config.MaxEndpoints)
	if err != nil {
		return nil
	}
	defer rows.Close()

	endpoints := []EndpointStats{}
	for rows.Next() {
		var operation string
		var total, errors int64
		var latencyP99 float64
		if err := rows.Scan(&operation, &total, &errors, &latencyP99); err != nil {
			continue
		}

		if total > 0 {
			redactedPath := e.redactor.RedactPath(operation)
			endpoints = append(endpoints, EndpointStats{
				Path:       redactedPath,
				ErrorRate:  float64(errors) / float64(total),
				LatencyP99: latencyP99,
				Count:      int(total),
			})
		}
	}

	return endpoints
}

// extractTimeAnalysis provides temporal context about the issue
func (e *Extractor) extractTimeAnalysis(service string, detectedAt time.Time) *TimeAnalysis {
	analysis := &TimeAnalysis{
		IssueStartedAt: detectedAt.Unix(),
		DurationMs:     time.Since(detectedAt).Milliseconds(),
	}

	// Calculate trend by comparing recent vs earlier error rates
	now := time.Now()
	recentSince := now.Add(-5 * time.Minute)
	earlierSince := now.Add(-15 * time.Minute)
	earlierUntil := now.Add(-5 * time.Minute)

	// Recent error rate (last 5 min)
	recentQuery := `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(status) = 'ERROR' THEN 1 ELSE 0 END) as errors
		FROM spans
		WHERE service = ? AND timestamp > ?
	`
	var recentTotal, recentErrors int64
	row := e.db.QueryRow(recentQuery, service, recentSince)
	row.Scan(&recentTotal, &recentErrors)

	// Earlier error rate (5-15 min ago)
	earlierQuery := `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN UPPER(status) = 'ERROR' THEN 1 ELSE 0 END) as errors
		FROM spans
		WHERE service = ? AND timestamp > ? AND timestamp < ?
	`
	var earlierTotal, earlierErrors int64
	row = e.db.QueryRow(earlierQuery, service, earlierSince, earlierUntil)
	row.Scan(&earlierTotal, &earlierErrors)

	// Determine trend
	if recentTotal > 0 && earlierTotal > 0 {
		recentRate := float64(recentErrors) / float64(recentTotal)
		earlierRate := float64(earlierErrors) / float64(earlierTotal)

		analysis.PeakErrorRate = recentRate
		if earlierRate > analysis.PeakErrorRate {
			analysis.PeakErrorRate = earlierRate
		}

		diff := recentRate - earlierRate
		if diff > 0.01 { // 1% increase
			analysis.Trend = "worsening"
		} else if diff < -0.01 { // 1% decrease
			analysis.Trend = "improving"
		} else {
			analysis.Trend = "stable"
		}
	} else {
		analysis.Trend = "unknown"
	}

	return analysis
}

// EstimateSize returns approximate JSON size of the context (for logging)
func (ctx *EnrichedContext) EstimateSize() int {
	size := 200 // Base struct overhead

	// Anomaly info
	size += len(ctx.Anomaly.Service) + len(ctx.Anomaly.Type) + len(ctx.Anomaly.Severity) + 50

	// Metrics
	size += 200

	// Error breakdown
	if ctx.ErrorBreakdown != nil {
		size += 100
	}

	// Correlations
	for _, c := range ctx.Correlations {
		size += len(c.Service) + 50
	}

	// Error logs
	for _, l := range ctx.ErrorLogs {
		size += len(l.Template) + len(l.Level) + len(l.ErrorType) + 50
	}

	// Failing traces
	for _, t := range ctx.FailingTraces {
		size += len(t.Operation) + len(t.ErrorType) + 50
	}

	// Endpoints
	for _, e := range ctx.AffectedEndpoints {
		size += len(e.Path) + 30
	}

	// Time analysis
	if ctx.TimeAnalysis != nil {
		size += 50
	}

	// Infrastructure state (can be significant)
	if ctx.InfraState != nil {
		size += 500 // Base overhead
		if ctx.InfraState.Kubernetes != nil {
			size += 300
			if ctx.InfraState.Kubernetes.Deployment != nil {
				size += 200 + len(ctx.InfraState.Kubernetes.Deployment.EnvVars)*30
			}
			size += len(ctx.InfraState.Kubernetes.Pods) * 150
			size += len(ctx.InfraState.Kubernetes.RecentEvents) * 100
		}
		if ctx.InfraState.Docker != nil {
			size += len(ctx.InfraState.Docker.Containers) * 200
		}
		if ctx.InfraState.Database != nil {
			size += 150
		}
		if ctx.InfraState.Cache != nil {
			size += 100
		}
	}

	return size
}
