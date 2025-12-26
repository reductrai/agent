package context

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"math"
	"sort"
	"strings"
	"time"
)

// FingerprintGenerator creates incident fingerprints for pattern matching
type FingerprintGenerator struct {
	db *sql.DB
}

// NewFingerprintGenerator creates a new fingerprint generator
func NewFingerprintGenerator(db *sql.DB) *FingerprintGenerator {
	return &FingerprintGenerator{db: db}
}

// Generate creates a fingerprint and looks up historical matches
func (f *FingerprintGenerator) Generate(
	service string,
	anomalyType string,
	correlations []ServiceCorrelation,
) IncidentFingerprint {
	// Create deterministic hash from incident characteristics
	hash := f.computeHash(service, anomalyType, correlations)

	// Query local DuckDB for historical matches
	matches := f.queryHistoricalMatches(hash)

	// Calculate confidence based on history
	confidence := f.calculateConfidence(matches)

	return IncidentFingerprint{
		Hash:           hash,
		MatchedHistory: matches,
		Confidence:     confidence,
	}
}

// computeHash generates a deterministic hash from incident characteristics
func (f *FingerprintGenerator) computeHash(
	service string,
	anomalyType string,
	correlations []ServiceCorrelation,
) string {
	// Sort correlated services for deterministic ordering
	correlatedServices := make([]string, 0, len(correlations))
	for _, c := range correlations {
		if c.ErrorRate > 0.01 { // Only include significantly failing services
			correlatedServices = append(correlatedServices, c.Service)
		}
	}
	sort.Strings(correlatedServices)

	// Create fingerprint data
	data := strings.Join([]string{
		service,
		anomalyType,
		strings.Join(correlatedServices, ","),
	}, "|")

	// Generate SHA256 and take first 16 hex chars
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8]) // 16 hex characters
}

// queryHistoricalMatches finds past incidents with the same fingerprint
func (f *FingerprintGenerator) queryHistoricalMatches(fingerprint string) []HistoricalMatch {
	matches := []HistoricalMatch{}

	query := `
		SELECT
			timestamp,
			duration_seconds,
			applied_fix,
			fix_succeeded,
			time_to_resolve_seconds
		FROM incident_history
		WHERE fingerprint = ?
		ORDER BY timestamp DESC
		LIMIT 10
	`

	rows, err := f.db.Query(query, fingerprint)
	if err != nil {
		return matches
	}
	defer rows.Close()

	for rows.Next() {
		var m HistoricalMatch
		var timestamp time.Time
		var durationSec sql.NullInt64
		var appliedFix sql.NullString
		var timeToResolve sql.NullInt64

		if err := rows.Scan(
			&timestamp,
			&durationSec,
			&appliedFix,
			&m.FixSucceeded,
			&timeToResolve,
		); err != nil {
			continue
		}

		m.Timestamp = timestamp.Unix()
		if durationSec.Valid {
			m.DurationSec = durationSec.Int64
		}
		if appliedFix.Valid {
			m.AppliedFix = appliedFix.String
		}
		if timeToResolve.Valid {
			m.TimeToResolve = timeToResolve.Int64
		}

		matches = append(matches, m)
	}

	return matches
}

// calculateConfidence determines confidence based on historical success rate
func (f *FingerprintGenerator) calculateConfidence(matches []HistoricalMatch) float64 {
	if len(matches) == 0 {
		return 0.0 // No history, no confidence
	}

	// Count successful fixes
	successCount := 0
	for _, m := range matches {
		if m.FixSucceeded {
			successCount++
		}
	}

	// Base confidence on success rate
	baseConfidence := float64(successCount) / float64(len(matches))

	// Boost confidence if we have more data points (max 20% boost)
	volumeBoost := math.Min(float64(len(matches))/10.0, 0.2)

	// Apply recency weighting - more recent successes count more
	recencyBoost := 0.0
	if len(matches) > 0 && matches[0].FixSucceeded {
		recencyBoost = 0.1 // 10% boost if most recent was successful
	}

	confidence := baseConfidence + volumeBoost + recencyBoost
	return math.Min(confidence, 1.0) // Cap at 100%
}

// RecordIncident stores an incident for future fingerprint matching
func (f *FingerprintGenerator) RecordIncident(
	fingerprint string,
	service string,
	anomalyType string,
	durationSec int64,
	appliedFix string,
	fixSucceeded bool,
	timeToResolveSec int64,
) error {
	query := `
		INSERT INTO incident_history (
			id, fingerprint, service, anomaly_type,
			timestamp, duration_seconds, applied_fix,
			fix_succeeded, time_to_resolve_seconds
		) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
	`

	// Generate a unique ID
	id := f.generateID(fingerprint)

	_, err := f.db.Exec(query,
		id, fingerprint, service, anomalyType,
		durationSec, appliedFix, fixSucceeded, timeToResolveSec,
	)
	return err
}

// generateID creates a unique incident ID
func (f *FingerprintGenerator) generateID(fingerprint string) string {
	data := fingerprint + string(rune(sha256.Sum256([]byte(fingerprint))[0]))
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16])
}

// GetSuccessfulFixes returns fixes that worked for this fingerprint
func (f *FingerprintGenerator) GetSuccessfulFixes(fingerprint string) []string {
	fixes := []string{}

	query := `
		SELECT DISTINCT applied_fix
		FROM incident_history
		WHERE fingerprint = ?
		  AND fix_succeeded = true
		  AND applied_fix IS NOT NULL
		  AND applied_fix != ''
		ORDER BY timestamp DESC
		LIMIT 5
	`

	rows, err := f.db.Query(query, fingerprint)
	if err != nil {
		return fixes
	}
	defer rows.Close()

	for rows.Next() {
		var fix string
		if err := rows.Scan(&fix); err == nil {
			fixes = append(fixes, fix)
		}
	}

	return fixes
}

// GetAverageTimeToResolve returns average resolution time for this pattern
func (f *FingerprintGenerator) GetAverageTimeToResolve(fingerprint string) int64 {
	var avgTime sql.NullFloat64

	query := `
		SELECT AVG(time_to_resolve_seconds)
		FROM incident_history
		WHERE fingerprint = ?
		  AND fix_succeeded = true
		  AND time_to_resolve_seconds > 0
	`

	row := f.db.QueryRow(query, fingerprint)
	if err := row.Scan(&avgTime); err != nil || !avgTime.Valid {
		return 0
	}

	return int64(avgTime.Float64)
}
