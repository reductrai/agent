package context

import (
	"database/sql"
	"fmt"
	"time"
)

// ScopeAnalyzer determines if an anomaly is isolated or part of a cascade
type ScopeAnalyzer struct {
	db *sql.DB
}

// NewScopeAnalyzer creates a new scope analyzer
func NewScopeAnalyzer(db *sql.DB) *ScopeAnalyzer {
	return &ScopeAnalyzer{db: db}
}

// Analyze determines the scope of an anomaly
// Returns: isolated | cascading | upstream | downstream
func (s *ScopeAnalyzer) Analyze(service string, correlations []ServiceCorrelation) ScopeAnalysis {
	if len(correlations) == 0 {
		return ScopeAnalysis{
			Scope:       "isolated",
			Reasoning:   fmt.Sprintf("%s is failing, no correlated services detected", service),
			HealthyDeps: s.getHealthyDependencies(service),
			FailingDeps: []string{},
		}
	}

	// Categorize correlations by direction
	var upstreamFailing []string
	var downstreamFailing []string
	var upstreamHealthy []string
	var downstreamHealthy []string

	for _, c := range correlations {
		if c.ErrorRate > 0.01 { // >1% error rate = failing
			if c.Direction == "upstream" {
				upstreamFailing = append(upstreamFailing, c.Service)
			} else {
				downstreamFailing = append(downstreamFailing, c.Service)
			}
		} else {
			if c.Direction == "upstream" {
				upstreamHealthy = append(upstreamHealthy, c.Service)
			} else {
				downstreamHealthy = append(downstreamHealthy, c.Service)
			}
		}
	}

	// Determine scope based on patterns
	scope := "isolated"
	var reasoning string

	if len(upstreamFailing) > 0 && len(downstreamFailing) == 0 {
		// Upstream is failing, downstream is healthy
		// Root cause is likely upstream
		scope = "upstream"
		reasoning = fmt.Sprintf("Upstream dependency %s is failing (%.1f%% errors). This is likely the root cause.",
			upstreamFailing[0], correlations[0].ErrorRate*100)
	} else if len(downstreamFailing) > 0 && len(upstreamFailing) == 0 {
		// Downstream is failing, upstream is healthy
		// This service is causing downstream failures
		scope = "downstream"
		reasoning = fmt.Sprintf("%s is causing failures in downstream services: %v",
			service, downstreamFailing)
	} else if len(upstreamFailing) > 0 && len(downstreamFailing) > 0 {
		// Both directions failing = cascading failure
		scope = "cascading"
		reasoning = fmt.Sprintf("Cascading failure detected. Failing services: upstream=%v, downstream=%v",
			upstreamFailing, downstreamFailing)
	} else {
		// No significant failures in dependencies
		scope = "isolated"
		reasoning = fmt.Sprintf("%s is failing in isolation. All dependencies are healthy.", service)
	}

	// Collect healthy deps
	healthyDeps := append(upstreamHealthy, downstreamHealthy...)
	failingDeps := append(upstreamFailing, downstreamFailing...)

	return ScopeAnalysis{
		Scope:       scope,
		Reasoning:   reasoning,
		HealthyDeps: healthyDeps,
		FailingDeps: failingDeps,
	}
}

// getHealthyDependencies queries for services that call or are called by this service
// and have low error rates
func (s *ScopeAnalyzer) getHealthyDependencies(service string) []string {
	healthy := []string{}
	since := time.Now().Add(-15 * time.Minute)

	// Find services with <1% error rate that interact with this service
	query := `
		SELECT DISTINCT other_service, error_rate FROM (
			-- Downstream: services this service calls
			SELECT
				child.service as other_service,
				CAST(SUM(CASE WHEN UPPER(child.status) = 'ERROR' THEN 1 ELSE 0 END) AS FLOAT) /
				NULLIF(CAST(COUNT(*) AS FLOAT), 0) as error_rate
			FROM spans parent
			JOIN spans child ON parent.span_id = child.parent_span_id
			WHERE parent.service = ?
			  AND child.service != ?
			  AND parent.timestamp > ?
			GROUP BY child.service

			UNION ALL

			-- Upstream: services that call this service
			SELECT
				parent.service as other_service,
				CAST(SUM(CASE WHEN UPPER(parent.status) = 'ERROR' THEN 1 ELSE 0 END) AS FLOAT) /
				NULLIF(CAST(COUNT(*) AS FLOAT), 0) as error_rate
			FROM spans parent
			JOIN spans child ON parent.span_id = child.parent_span_id
			WHERE child.service = ?
			  AND parent.service != ?
			  AND parent.timestamp > ?
			GROUP BY parent.service
		) sub
		WHERE error_rate < 0.01 OR error_rate IS NULL
		LIMIT 10
	`

	rows, err := s.db.Query(query, service, service, since, service, service, since)
	if err != nil {
		return healthy
	}
	defer rows.Close()

	for rows.Next() {
		var svc string
		var errorRate sql.NullFloat64
		if err := rows.Scan(&svc, &errorRate); err == nil {
			healthy = append(healthy, svc)
		}
	}

	return healthy
}

// AnalyzeWithMetrics provides a more detailed scope analysis using service metrics
func (s *ScopeAnalyzer) AnalyzeWithMetrics(service string, correlations []ServiceCorrelation, metrics ServiceMetrics) ScopeAnalysis {
	scope := s.Analyze(service, correlations)

	// Enhance reasoning with metrics context
	if metrics.ErrorRate > metrics.ErrorRateBaseline*5 {
		scope.Reasoning += fmt.Sprintf(" Error rate is %.1fx above baseline.", metrics.ErrorRate/metrics.ErrorRateBaseline)
	}
	if metrics.LatencyP99 > metrics.LatencyBaseline*3 {
		scope.Reasoning += fmt.Sprintf(" Latency P99 is %.1fx above baseline.", metrics.LatencyP99/metrics.LatencyBaseline)
	}

	return scope
}
