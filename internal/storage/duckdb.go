package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/reductrai/agent/internal/compression"
	"github.com/reductrai/agent/pkg/types"
)

// Storage tier constants
const (
	TierHot  = "hot"  // Recent data (< 1 hour) - fast queries
	TierWarm = "warm" // Older data (1h - 7 days) - standard
	TierCold = "cold" // Old data (> 7 days) - archived

	// Auto-tiering thresholds (same for all license tiers)
	HotToWarmAge  = 1 * time.Hour      // Move to warm after 1 hour
	WarmToColdAge = 7 * 24 * time.Hour // Move to cold after 7 days

	// Tiering job interval
	TieringInterval = 5 * time.Minute
)

// License tier retention periods
var RetentionByLicenseTier = map[string]time.Duration{
	"FREE":       30 * 24 * time.Hour,  // 30 days
	"PRO":        90 * 24 * time.Hour,  // 90 days
	"BUSINESS":   180 * 24 * time.Hour, // 180 days (6 months)
	"ENTERPRISE": 365 * 24 * time.Hour, // 1 year
}

// Default retention if tier unknown
const DefaultRetention = 30 * 24 * time.Hour

// DuckDB wraps DuckDB connection and provides storage operations with auto-tiering
type DuckDB struct {
	db            *sql.DB
	mu            sync.RWMutex
	path          string
	licenseTier   string // FREE, PRO, BUSINESS, ENTERPRISE
	tieringCtx    context.Context
	tieringCancel context.CancelFunc
	compressor    *compression.EngineV2 // V2 compression engine
}

// NewDuckDB creates a new DuckDB storage instance with auto-tiering (defaults to FREE tier)
func NewDuckDB(dataDir string) (*DuckDB, error) {
	return NewDuckDBWithTier(dataDir, "FREE")
}

// NewDuckDBWithTier creates a new DuckDB storage instance with specified license tier
func NewDuckDBWithTier(dataDir string, licenseTier string) (*DuckDB, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, "reductrai.db")

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store := &DuckDB{
		db:            db,
		path:          dbPath,
		licenseTier:   licenseTier,
		tieringCtx:    ctx,
		tieringCancel: cancel,
		compressor:    compression.NewEngineV2(), // Initialize V2 compression
	}

	// Initialize schema
	if err := store.initSchema(); err != nil {
		db.Close()
		cancel()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Start auto-tiering background job
	go store.runAutoTiering()

	return store, nil
}

// SetLicenseTier updates the license tier (affects retention policy)
func (d *DuckDB) SetLicenseTier(tier string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.licenseTier = tier
}

// GetRetentionPeriod returns the retention period for the current license tier
func (d *DuckDB) GetRetentionPeriod() time.Duration {
	if retention, ok := RetentionByLicenseTier[d.licenseTier]; ok {
		return retention
	}
	return DefaultRetention
}

func (d *DuckDB) initSchema() error {
	// Load JSON extension
	if _, err := d.db.Exec("INSTALL json; LOAD json;"); err != nil {
		// Extension might already be loaded, continue
	}

	schemas := []string{
		// Metrics table with tier column
		`CREATE TABLE IF NOT EXISTS metrics (
			timestamp TIMESTAMP,
			service VARCHAR,
			name VARCHAR,
			value DOUBLE,
			tags JSON,
			tier VARCHAR DEFAULT 'hot'
		)`,
		// Logs table with tier column
		`CREATE TABLE IF NOT EXISTS logs (
			timestamp TIMESTAMP,
			service VARCHAR,
			level VARCHAR,
			message VARCHAR,
			tags JSON,
			tier VARCHAR DEFAULT 'hot'
		)`,
		// Spans table with tier column
		`CREATE TABLE IF NOT EXISTS spans (
			timestamp TIMESTAMP,
			trace_id VARCHAR,
			span_id VARCHAR,
			parent_span_id VARCHAR,
			service VARCHAR,
			operation VARCHAR,
			duration_us BIGINT,
			status VARCHAR,
			tags JSON,
			tier VARCHAR DEFAULT 'hot'
		)`,
		// Compressed data table for V2 compression storage
		`CREATE TABLE IF NOT EXISTS compressed_data (
			id VARCHAR PRIMARY KEY,
			data_type VARCHAR,
			time_start TIMESTAMP,
			time_end TIMESTAMP,
			original_size BIGINT,
			compressed_size BIGINT,
			compression_ratio DOUBLE,
			format VARCHAR,
			technique VARCHAR,
			data BLOB,
			metadata JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		// Indexes for fast queries
		`CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_metrics_service ON metrics(service)`,
		`CREATE INDEX IF NOT EXISTS idx_metrics_tier ON metrics(tier)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_tier ON logs(tier)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_ts ON spans(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_service ON spans(service)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_tier ON spans(tier)`,
		`CREATE INDEX IF NOT EXISTS idx_compressed_ts ON compressed_data(time_start, time_end)`,
		`CREATE INDEX IF NOT EXISTS idx_compressed_type ON compressed_data(data_type)`,
		// Incident history table for fingerprint pattern matching
		`CREATE TABLE IF NOT EXISTS incident_history (
			id VARCHAR PRIMARY KEY,
			fingerprint VARCHAR NOT NULL,
			service VARCHAR NOT NULL,
			anomaly_type VARCHAR NOT NULL,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			duration_seconds INTEGER,
			applied_fix VARCHAR,
			fix_succeeded BOOLEAN DEFAULT false,
			time_to_resolve_seconds INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_incident_fingerprint ON incident_history(fingerprint)`,
		`CREATE INDEX IF NOT EXISTS idx_incident_service ON incident_history(service)`,
	}

	for _, schema := range schemas {
		if _, err := d.db.Exec(schema); err != nil {
			return err
		}
	}

	// Try to add tier column if it doesn't exist (for existing databases)
	d.db.Exec("ALTER TABLE metrics ADD COLUMN IF NOT EXISTS tier VARCHAR DEFAULT 'hot'")
	d.db.Exec("ALTER TABLE logs ADD COLUMN IF NOT EXISTS tier VARCHAR DEFAULT 'hot'")
	d.db.Exec("ALTER TABLE spans ADD COLUMN IF NOT EXISTS tier VARCHAR DEFAULT 'hot'")

	return nil
}

// Close closes the database connection and stops auto-tiering
func (d *DuckDB) Close() error {
	// Stop auto-tiering
	if d.tieringCancel != nil {
		d.tieringCancel()
	}
	return d.db.Close()
}

// runAutoTiering runs the automatic tiering background job
func (d *DuckDB) runAutoTiering() {
	ticker := time.NewTicker(TieringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.tieringCtx.Done():
			return
		case <-ticker.C:
			d.runTieringJob()
		}
	}
}

// runTieringJob moves data between tiers and cleans up old data
func (d *DuckDB) runTieringJob() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Move hot → warm (data older than 1 hour)
	warmCutoff := now.Add(-HotToWarmAge)
	d.moveTier("spans", TierHot, TierWarm, warmCutoff)
	d.moveTier("logs", TierHot, TierWarm, warmCutoff)
	d.moveTier("metrics", TierHot, TierWarm, warmCutoff)

	// Move warm → cold (data older than 7 days)
	coldCutoff := now.Add(-WarmToColdAge)
	d.moveTier("spans", TierWarm, TierCold, coldCutoff)
	d.moveTier("logs", TierWarm, TierCold, coldCutoff)
	d.moveTier("metrics", TierWarm, TierCold, coldCutoff)

	// Delete data older than retention period (based on license tier)
	retention := d.getRetentionPeriodUnsafe()
	retentionCutoff := now.Add(-retention)
	d.deleteOldData("spans", retentionCutoff, retention)
	d.deleteOldData("logs", retentionCutoff, retention)
	d.deleteOldData("metrics", retentionCutoff, retention)
}

// getRetentionPeriodUnsafe returns retention without locking (caller must hold lock)
func (d *DuckDB) getRetentionPeriodUnsafe() time.Duration {
	if retention, ok := RetentionByLicenseTier[d.licenseTier]; ok {
		return retention
	}
	return DefaultRetention
}

// moveTier updates tier for data older than cutoff
// When moving from HOT to WARM, data is compressed and stored in compressed_data table
func (d *DuckDB) moveTier(table, fromTier, toTier string, cutoff time.Time) {
	// When moving HOT → WARM, compress the data
	if fromTier == TierHot && toTier == TierWarm {
		d.compressAndMoveTier(table, cutoff)
		return
	}

	// For WARM → COLD, just update the tier (data already compressed)
	// We mark cold data by appending "_cold" to data_type
	query := `UPDATE compressed_data SET data_type = ? WHERE data_type = ? AND time_end < ?`
	result, err := d.db.Exec(query, table+"_cold", table, cutoff)
	if err != nil {
		return
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		fmt.Printf("[Storage] Moved %d %s blocks from %s to %s tier\n", rows, table, fromTier, toTier)
	}
}

// compressAndMoveTier compresses HOT data and moves it to compressed storage
func (d *DuckDB) compressAndMoveTier(table string, cutoff time.Time) {
	// Get time range for this compression batch
	// Find the oldest timestamp in HOT tier
	var oldestTime, newestTime time.Time

	query := fmt.Sprintf(
		`SELECT MIN(timestamp), MAX(timestamp) FROM %s WHERE tier = 'hot' AND timestamp < ?`,
		table,
	)
	row := d.db.QueryRow(query, cutoff)
	if err := row.Scan(&oldestTime, &newestTime); err != nil || oldestTime.IsZero() {
		return // No data to compress
	}

	var stats *compression.CompressionStats
	var err error

	switch table {
	case "spans":
		stats, err = d.compressHotSpans(oldestTime, cutoff)
	case "logs":
		stats, err = d.compressHotLogs(oldestTime, cutoff)
	case "metrics":
		stats, err = d.compressHotMetrics(oldestTime, cutoff)
	}

	if err != nil {
		fmt.Printf("[Storage] Compression failed for %s: %v\n", table, err)
		return
	}

	if stats != nil {
		fmt.Printf("[Storage] Compressed %s: %d bytes → %d bytes (%.1f%% reduction)\n",
			table, stats.OriginalSize, stats.CompressedSize, stats.CompressionPercent)

		// Delete the raw data after successful compression
		deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE tier = 'hot' AND timestamp < ?`, table)
		result, _ := d.db.Exec(deleteQuery, cutoff)
		if rows, _ := result.RowsAffected(); rows > 0 {
			fmt.Printf("[Storage] Removed %d raw %s records (now compressed)\n", rows, table)
		}
	}
}

// compressHotSpans fetches and compresses HOT spans
func (d *DuckDB) compressHotSpans(timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	query := `SELECT timestamp, trace_id, span_id, parent_span_id, service, operation, duration_us, status, CAST(tags AS VARCHAR)
			  FROM spans WHERE tier = 'hot' AND timestamp >= ? AND timestamp < ?`

	rows, err := d.db.Query(query, timeStart, timeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spans []types.Span
	for rows.Next() {
		var s types.Span
		var durationUs int64
		var tagsJSON string

		if err := rows.Scan(&s.StartTime, &s.TraceID, &s.SpanID, &s.ParentSpanID,
			&s.Service, &s.Operation, &durationUs, &s.Status, &tagsJSON); err != nil {
			continue
		}
		s.Duration = time.Duration(durationUs) * time.Microsecond
		s.Tags = jsonToMap(tagsJSON)
		spans = append(spans, s)
	}

	if len(spans) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compSpans := make([]compression.Span, len(spans))
	for i, s := range spans {
		attrs := make(map[string]interface{})
		for k, v := range s.Tags {
			attrs[k] = v
		}
		compSpans[i] = compression.Span{
			TraceID:      s.TraceID,
			SpanID:       s.SpanID,
			ParentSpanID: s.ParentSpanID,
			Name:         s.Operation,
			StartTime:    s.StartTime.UnixNano(),
			EndTime:      s.StartTime.Add(s.Duration).UnixNano(),
			Status:       s.Status,
			Service:      s.Service,
			Attributes:   attrs,
		}
	}

	// Compress
	block, err := d.compressor.CompressSpans(compSpans)
	if err != nil {
		return nil, err
	}

	// Store compressed data
	id := fmt.Sprintf("spans_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "spans", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, err
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	return &compression.CompressionStats{
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionPercent: compressionPercent,
	}, nil
}

// compressHotLogs fetches and compresses HOT logs
func (d *DuckDB) compressHotLogs(timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	query := `SELECT timestamp, service, level, message, CAST(tags AS VARCHAR)
			  FROM logs WHERE tier = 'hot' AND timestamp >= ? AND timestamp < ?`

	rows, err := d.db.Query(query, timeStart, timeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []types.LogEntry
	for rows.Next() {
		var l types.LogEntry
		var tagsJSON string

		if err := rows.Scan(&l.Timestamp, &l.Service, &l.Level, &l.Message, &tagsJSON); err != nil {
			continue
		}
		l.Tags = jsonToMap(tagsJSON)
		logs = append(logs, l)
	}

	if len(logs) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compLogs := make([]compression.LogLine, len(logs))
	for i, l := range logs {
		compLogs[i] = compression.LogLine{
			Timestamp: l.Timestamp.Format(time.RFC3339),
			Level:     l.Level,
			Service:   l.Service,
			Message:   l.Message,
			Raw:       l.Message,
		}
	}

	// Compress
	block, err := d.compressor.CompressLogs(compLogs)
	if err != nil {
		return nil, err
	}

	// Store compressed data
	id := fmt.Sprintf("logs_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "logs", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, err
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	return &compression.CompressionStats{
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionPercent: compressionPercent,
	}, nil
}

// compressHotMetrics fetches and compresses HOT metrics
func (d *DuckDB) compressHotMetrics(timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	query := `SELECT timestamp, service, name, value, CAST(tags AS VARCHAR)
			  FROM metrics WHERE tier = 'hot' AND timestamp >= ? AND timestamp < ?`

	rows, err := d.db.Query(query, timeStart, timeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []types.Metric
	for rows.Next() {
		var m types.Metric
		var tagsJSON string

		if err := rows.Scan(&m.Timestamp, &m.Service, &m.Name, &m.Value, &tagsJSON); err != nil {
			continue
		}
		m.Tags = jsonToMap(tagsJSON)
		metrics = append(metrics, m)
	}

	if len(metrics) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compMetrics := make([]compression.MetricPoint, len(metrics))
	for i, m := range metrics {
		labels := make(map[string]string)
		for k, v := range m.Tags {
			labels[k] = v
		}
		compMetrics[i] = compression.MetricPoint{
			Metric:    m.Name,
			Labels:    labels,
			Value:     m.Value,
			Timestamp: m.Timestamp.UnixMilli(),
		}
	}

	// Compress
	block, err := d.compressor.CompressMetrics(compMetrics)
	if err != nil {
		return nil, err
	}

	// Store compressed data
	id := fmt.Sprintf("metrics_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "metrics", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, err
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	return &compression.CompressionStats{
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionPercent: compressionPercent,
	}, nil
}

// jsonToMap converts a JSON string to map[string]string
func jsonToMap(jsonStr string) map[string]string {
	result := make(map[string]string)
	if jsonStr == "" || jsonStr == "{}" {
		return result
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return result
	}
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		} else {
			result[k] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

// deleteOldData removes data older than cutoff
func (d *DuckDB) deleteOldData(table string, cutoff time.Time, retention time.Duration) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE timestamp < ?`, table)
	result, err := d.db.Exec(query, cutoff)
	if err != nil {
		return
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		days := int(retention.Hours() / 24)
		fmt.Printf("[Storage] Deleted %d expired %s records (retention: %d days, tier: %s)\n",
			rows, table, days, d.licenseTier)
	}
}

// GetTierStats returns storage statistics by tier
func (d *DuckDB) GetTierStats() map[string]map[string]int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := map[string]map[string]int64{
		"spans":   {},
		"logs":    {},
		"metrics": {},
	}

	for _, table := range []string{"spans", "logs", "metrics"} {
		query := fmt.Sprintf(`SELECT tier, COUNT(*) as count FROM %s GROUP BY tier`, table)
		rows, err := d.db.Query(query)
		if err != nil {
			continue
		}

		for rows.Next() {
			var tier string
			var count int64
			if err := rows.Scan(&tier, &count); err == nil {
				stats[table][tier] = count
			}
		}
		rows.Close()
	}

	return stats
}

// InsertMetric inserts a metric
func (d *DuckDB) InsertMetric(m types.Metric) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO metrics (timestamp, service, name, value, tags) VALUES (?, ?, ?, ?, ?)`,
		m.Timestamp, m.Service, m.Name, m.Value, mapToJSON(m.Tags),
	)
	return err
}

// InsertLog inserts a log entry
func (d *DuckDB) InsertLog(l types.LogEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO logs (timestamp, service, level, message, tags) VALUES (?, ?, ?, ?, ?)`,
		l.Timestamp, l.Service, l.Level, l.Message, mapToJSON(l.Tags),
	)
	return err
}

// InsertSpan inserts a trace span (for low-volume, use InsertSpansBatch for high-volume)
func (d *DuckDB) InsertSpan(s types.Span) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO spans (timestamp, trace_id, span_id, parent_span_id, service, operation, duration_us, status, tags)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.StartTime, s.TraceID, s.SpanID, s.ParentSpanID, s.Service, s.Operation,
		s.Duration.Microseconds(), s.Status, mapToJSON(s.Tags),
	)
	return err
}

// InsertSpansBatch inserts multiple spans in a single transaction (high-volume)
func (d *DuckDB) InsertSpansBatch(spans []types.Span) error {
	if len(spans) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT INTO spans (timestamp, trace_id, span_id, parent_span_id, service, operation, duration_us, status, tags)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, s := range spans {
		_, err := stmt.Exec(
			s.StartTime, s.TraceID, s.SpanID, s.ParentSpanID, s.Service, s.Operation,
			s.Duration.Microseconds(), s.Status, mapToJSON(s.Tags),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert span: %w", err)
		}
	}

	return tx.Commit()
}

// InsertLogsBatch inserts multiple logs in a single transaction (high-volume)
func (d *DuckDB) InsertLogsBatch(logs []types.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT INTO logs (timestamp, service, level, message, tags) VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, l := range logs {
		_, err := stmt.Exec(l.Timestamp, l.Service, l.Level, l.Message, mapToJSON(l.Tags))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert log: %w", err)
		}
	}

	return tx.Commit()
}

// InsertMetricsBatch inserts multiple metrics in a single transaction (high-volume)
func (d *DuckDB) InsertMetricsBatch(metrics []types.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT INTO metrics (timestamp, service, name, value, tags) VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, m := range metrics {
		_, err := stmt.Exec(m.Timestamp, m.Service, m.Name, m.Value, mapToJSON(m.Tags))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert metric: %w", err)
		}
	}

	return tx.Commit()
}

// GetServiceHealth returns aggregated health metrics for all services
func (d *DuckDB) GetServiceHealth() []types.ServiceHealth {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Get services from last 5 minutes
	since := time.Now().Add(-5 * time.Minute)

	// Query error rates and latencies from spans
	query := `
		SELECT
			service,
			COUNT(*) as total,
			SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_us) as p50,
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_us) as p99
		FROM spans
		WHERE timestamp > ?
		GROUP BY service
	`

	rows, err := d.db.Query(query, since)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var services []types.ServiceHealth
	for rows.Next() {
		var svc types.ServiceHealth
		var total, errors int64
		var p50, p99 float64

		if err := rows.Scan(&svc.Service, &total, &errors, &p50, &p99); err != nil {
			continue
		}

		if total > 0 {
			svc.ErrorRate = float64(errors) / float64(total)
		}
		svc.LatencyP50Ms = p50 / 1000 // us to ms
		svc.LatencyP99Ms = p99 / 1000
		svc.RequestsPerMin = float64(total) / 5 // 5 minute window

		services = append(services, svc)
	}

	return services
}

// GetRecentErrors returns recent error logs
func (d *DuckDB) GetRecentErrors(service string, limit int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
		SELECT message FROM logs
		WHERE service = ? AND level IN ('error', 'ERROR', 'fatal', 'FATAL')
		ORDER BY timestamp DESC
		LIMIT ?
	`

	rows, err := d.db.Query(query, service, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var errors []string
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err == nil {
			errors = append(errors, msg)
		}
	}

	return errors
}

// GetServiceDependencies returns services this service calls
func (d *DuckDB) GetServiceDependencies(service string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Find downstream services by looking at parent-child span relationships
	query := `
		SELECT DISTINCT child.service
		FROM spans parent
		JOIN spans child ON parent.span_id = child.parent_span_id
		WHERE parent.service = ? AND child.service != ?
	`

	rows, err := d.db.Query(query, service, service)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err == nil {
			deps = append(deps, dep)
		}
	}

	return deps
}

// Query runs a raw SQL query (for CLI)
func (d *DuckDB) Query(sql string) (*sql.Rows, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db.Query(sql)
}

// GetDB returns the underlying sql.DB for direct queries
func (d *DuckDB) GetDB() *sql.DB {
	return d.db
}

func mapToJSON(m map[string]string) string {
	if m == nil {
		return "{}"
	}
	// Simple JSON encoding for tags
	result := "{"
	first := true
	for k, v := range m {
		if !first {
			result += ","
		}
		result += fmt.Sprintf(`"%s":"%s"`, k, v)
		first = false
	}
	result += "}"
	return result
}

// =============================================================================
// V2 Compression Methods
// =============================================================================

// GetCompressor returns the V2 compression engine
func (d *DuckDB) GetCompressor() *compression.EngineV2 {
	return d.compressor
}

// CompressAndStoreSpans compresses spans and stores them in compressed_data table
func (d *DuckDB) CompressAndStoreSpans(spans []types.Span, timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	if len(spans) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compSpans := make([]compression.Span, len(spans))
	for i, s := range spans {
		attrs := make(map[string]interface{})
		for k, v := range s.Tags {
			attrs[k] = v
		}
		compSpans[i] = compression.Span{
			TraceID:      s.TraceID,
			SpanID:       s.SpanID,
			ParentSpanID: s.ParentSpanID,
			Name:         s.Operation,
			StartTime:    s.StartTime.UnixNano(),
			EndTime:      s.StartTime.Add(s.Duration).UnixNano(),
			Status:       s.Status,
			Service:      s.Service,
			Attributes:   attrs,
		}
	}

	// Compress
	block, err := d.compressor.CompressSpans(compSpans)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	// Store in compressed_data table
	id := fmt.Sprintf("spans_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "spans", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to store compressed data: %w", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	return &compression.CompressionStats{
		Format:             block.Metadata.Format,
		AdapterUsed:        "SpanPatternCompressor",
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionRatio:   block.CompressionRatio,
		CompressionPercent: compressionPercent,
		Technique:          block.Metadata.Technique,
	}, nil
}

// CompressAndStoreLogs compresses logs and stores them in compressed_data table
func (d *DuckDB) CompressAndStoreLogs(logs []types.LogEntry, timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compLogs := make([]compression.LogLine, len(logs))
	for i, l := range logs {
		compLogs[i] = compression.LogLine{
			Timestamp: l.Timestamp.Format(time.RFC3339),
			Level:     l.Level,
			Service:   l.Service,
			Message:   l.Message,
			Raw:       l.Message,
		}
	}

	// Compress
	block, err := d.compressor.CompressLogs(compLogs)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	// Store in compressed_data table
	id := fmt.Sprintf("logs_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "logs", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to store compressed data: %w", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	return &compression.CompressionStats{
		Format:             block.Metadata.Format,
		AdapterUsed:        "ContextualDictionaryCompressor",
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionRatio:   block.CompressionRatio,
		CompressionPercent: compressionPercent,
		Technique:          block.Metadata.Technique,
	}, nil
}

// CompressAndStoreMetrics compresses metrics and stores them in compressed_data table
func (d *DuckDB) CompressAndStoreMetrics(metrics []types.Metric, timeStart, timeEnd time.Time) (*compression.CompressionStats, error) {
	if len(metrics) == 0 {
		return nil, nil
	}

	// Convert to compression format
	compMetrics := make([]compression.MetricPoint, len(metrics))
	for i, m := range metrics {
		labels := make(map[string]string)
		for k, v := range m.Tags {
			labels[k] = v
		}
		compMetrics[i] = compression.MetricPoint{
			Metric:    m.Name,
			Labels:    labels,
			Value:     m.Value,
			Timestamp: m.Timestamp.UnixMilli(),
		}
	}

	// Compress
	block, err := d.compressor.CompressMetrics(compMetrics)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	// Store in compressed_data table
	id := fmt.Sprintf("metrics_%d_%d", timeStart.Unix(), timeEnd.Unix())
	metadata, _ := json.Marshal(block.Metadata)

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.db.Exec(
		`INSERT OR REPLACE INTO compressed_data
		 (id, data_type, time_start, time_end, original_size, compressed_size, compression_ratio, format, technique, data, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "metrics", timeStart, timeEnd,
		block.OriginalSize, block.CompressedSize, block.CompressionRatio,
		block.Metadata.Format, block.Metadata.Technique, block.Data, string(metadata),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to store compressed data: %w", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	return &compression.CompressionStats{
		Format:             block.Metadata.Format,
		AdapterUsed:        "TimeSeriesAggregator",
		OriginalSize:       block.OriginalSize,
		CompressedSize:     block.CompressedSize,
		CompressionRatio:   block.CompressionRatio,
		CompressionPercent: compressionPercent,
		Technique:          block.Metadata.Technique,
	}, nil
}

// GetCompressionStats returns compression statistics for stored data
func (d *DuckDB) GetCompressionStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := make(map[string]interface{})

	// Get total compressed data stats
	query := `
		SELECT
			data_type,
			COUNT(*) as blocks,
			SUM(original_size) as total_original,
			SUM(compressed_size) as total_compressed,
			AVG(compression_ratio) as avg_ratio
		FROM compressed_data
		GROUP BY data_type
	`

	rows, err := d.db.Query(query)
	if err != nil {
		stats["error"] = err.Error()
		return stats
	}
	defer rows.Close()

	byType := make(map[string]map[string]interface{})
	var totalOriginal, totalCompressed int64

	for rows.Next() {
		var dataType string
		var blocks int
		var original, compressed int64
		var ratio float64

		if err := rows.Scan(&dataType, &blocks, &original, &compressed, &ratio); err != nil {
			continue
		}

		totalOriginal += original
		totalCompressed += compressed

		byType[dataType] = map[string]interface{}{
			"blocks":            blocks,
			"original_bytes":    original,
			"compressed_bytes":  compressed,
			"compression_ratio": ratio,
			"savings_percent":   (1.0 - float64(compressed)/float64(original)) * 100,
		}
	}

	stats["by_type"] = byType
	stats["total_original_bytes"] = totalOriginal
	stats["total_compressed_bytes"] = totalCompressed

	if totalOriginal > 0 {
		stats["overall_compression_percent"] = (1.0 - float64(totalCompressed)/float64(totalOriginal)) * 100
	}

	// Add engine info
	stats["engine_info"] = d.compressor.GetAdapterInfo()

	return stats
}
