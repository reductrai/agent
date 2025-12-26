package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/reductrai/agent/pkg/types"
)

func TestCompressionOnTierTransition(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "reductrai-tier-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create DuckDB instance
	db, err := NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	// Insert test spans (simulate data that's older than 1 hour)
	oldTime := time.Now().Add(-2 * time.Hour)

	spans := make([]types.Span, 1000)
	services := []string{"api-gateway", "user-service", "payment-service"}
	operations := []string{"GET /api/users", "POST /api/orders", "GET /api/products"}

	for i := 0; i < 1000; i++ {
		spans[i] = types.Span{
			TraceID:      fmt.Sprintf("trace-%d", i/10),
			SpanID:       fmt.Sprintf("span-%d", i),
			ParentSpanID: fmt.Sprintf("span-%d", i-1),
			Service:      services[i%len(services)],
			Operation:    operations[i%len(operations)],
			StartTime:    oldTime.Add(time.Duration(i) * time.Millisecond),
			Duration:     time.Millisecond * 50,
			Status:       "OK",
			Tags:         map[string]string{"http.method": "GET", "http.status_code": "200"},
		}
	}

	// Insert spans
	if err := db.InsertSpansBatch(spans); err != nil {
		t.Fatalf("Failed to insert spans: %v", err)
	}

	// Insert test logs
	logs := make([]types.LogEntry, 1000)
	levels := []string{"INFO", "DEBUG", "WARN", "ERROR"}

	for i := 0; i < 1000; i++ {
		logs[i] = types.LogEntry{
			Timestamp: oldTime.Add(time.Duration(i) * time.Millisecond),
			Level:     levels[i%len(levels)],
			Service:   services[i%len(services)],
			Message:   fmt.Sprintf("Processing request %d for user %d", i, i%100),
			Tags:      map[string]string{"env": "production"},
		}
	}

	if err := db.InsertLogsBatch(logs); err != nil {
		t.Fatalf("Failed to insert logs: %v", err)
	}

	// Insert test metrics
	metrics := make([]types.Metric, 1000)
	metricNames := []string{"http_requests_total", "http_request_duration_seconds", "process_cpu_seconds_total"}

	for i := 0; i < 1000; i++ {
		metrics[i] = types.Metric{
			Timestamp: oldTime.Add(time.Duration(i) * time.Millisecond),
			Name:      metricNames[i%len(metricNames)],
			Value:     float64(i % 100),
			Service:   services[i%len(services)],
			Tags:      map[string]string{"env": "production", "region": "us-east-1"},
		}
	}

	if err := db.InsertMetricsBatch(metrics); err != nil {
		t.Fatalf("Failed to insert metrics: %v", err)
	}

	// Verify data is in HOT tier
	tierStats := db.GetTierStats()
	fmt.Printf("\n=== Before Tier Transition ===\n")
	fmt.Printf("Spans HOT:   %d\n", tierStats["spans"]["hot"])
	fmt.Printf("Logs HOT:    %d\n", tierStats["logs"]["hot"])
	fmt.Printf("Metrics HOT: %d\n", tierStats["metrics"]["hot"])

	if tierStats["spans"]["hot"] != 1000 {
		t.Errorf("Expected 1000 spans in HOT tier, got %d", tierStats["spans"]["hot"])
	}

	// Trigger tier transition (data older than 1 hour should be compressed)
	fmt.Printf("\n=== Triggering Tier Transition ===\n")
	db.runTieringJob()

	// Verify data was compressed and moved
	tierStats = db.GetTierStats()
	fmt.Printf("\n=== After Tier Transition ===\n")
	fmt.Printf("Spans HOT:   %d\n", tierStats["spans"]["hot"])
	fmt.Printf("Logs HOT:    %d\n", tierStats["logs"]["hot"])
	fmt.Printf("Metrics HOT: %d\n", tierStats["metrics"]["hot"])

	// HOT tier should be empty now
	if tierStats["spans"]["hot"] != 0 {
		t.Errorf("Expected 0 spans in HOT tier after compression, got %d", tierStats["spans"]["hot"])
	}
	if tierStats["logs"]["hot"] != 0 {
		t.Errorf("Expected 0 logs in HOT tier after compression, got %d", tierStats["logs"]["hot"])
	}
	if tierStats["metrics"]["hot"] != 0 {
		t.Errorf("Expected 0 metrics in HOT tier after compression, got %d", tierStats["metrics"]["hot"])
	}

	// Verify compressed data exists
	compStats := db.GetCompressionStats()
	fmt.Printf("\n=== Compression Stats ===\n")

	if byType, ok := compStats["by_type"].(map[string]map[string]interface{}); ok {
		for dataType, stats := range byType {
			fmt.Printf("%s: %d blocks, %.1f%% savings\n",
				dataType, stats["blocks"], stats["savings_percent"])
		}
	}

	if compStats["total_original_bytes"].(int64) == 0 {
		t.Error("Expected compressed data to exist, but total_original_bytes is 0")
	}

	overallCompression := compStats["overall_compression_percent"].(float64)
	fmt.Printf("\nOverall compression: %.1f%%\n", overallCompression)

	if overallCompression < 70 {
		t.Errorf("Expected >70%% overall compression, got %.1f%%", overallCompression)
	}

	fmt.Printf("\n✓ Tier transition with compression working correctly\n")
}

func TestTierTransitionPreservesRecentData(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "reductrai-tier-preserve-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create DuckDB instance
	db, err := NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	// Insert recent spans (within last hour - should NOT be compressed)
	recentTime := time.Now().Add(-30 * time.Minute)

	spans := make([]types.Span, 100)
	for i := 0; i < 100; i++ {
		spans[i] = types.Span{
			TraceID:   fmt.Sprintf("trace-%d", i),
			SpanID:    fmt.Sprintf("span-%d", i),
			Service:   "recent-service",
			Operation: "GET /api/recent",
			StartTime: recentTime.Add(time.Duration(i) * time.Millisecond),
			Duration:  time.Millisecond * 50,
			Status:    "OK",
		}
	}

	if err := db.InsertSpansBatch(spans); err != nil {
		t.Fatalf("Failed to insert spans: %v", err)
	}

	// Trigger tier transition
	db.runTieringJob()

	// Verify recent data is still in HOT tier
	tierStats := db.GetTierStats()

	if tierStats["spans"]["hot"] != 100 {
		t.Errorf("Expected 100 recent spans to remain in HOT tier, got %d", tierStats["spans"]["hot"])
	}

	fmt.Printf("✓ Recent data (< 1 hour old) preserved in HOT tier\n")
}

func TestCompressionStatsEndpoint(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "reductrai-stats-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	// Get stats on empty database
	stats := db.GetCompressionStats()

	// Should have engine info even with no data
	if stats["engine_info"] == nil {
		t.Error("Expected engine_info in compression stats")
	}

	fmt.Printf("✓ Compression stats endpoint working\n")
}

func TestCompressedDataTable(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "reductrai-table-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	// Verify compressed_data table exists
	rows, err := db.Query("SELECT COUNT(*) FROM compressed_data")
	if err != nil {
		t.Fatalf("compressed_data table should exist: %v", err)
	}
	rows.Close()

	// Verify table has correct columns
	rows, err = db.Query("DESCRIBE compressed_data")
	if err != nil {
		t.Fatalf("Failed to describe table: %v", err)
	}
	defer rows.Close()

	expectedColumns := map[string]bool{
		"id": false, "data_type": false, "time_start": false, "time_end": false,
		"original_size": false, "compressed_size": false, "compression_ratio": false,
		"format": false, "technique": false, "data": false, "metadata": false,
	}

	for rows.Next() {
		var colName, colType string
		var nullable, key, defaultVal, extra interface{}
		if err := rows.Scan(&colName, &colType, &nullable, &key, &defaultVal, &extra); err != nil {
			continue
		}
		if _, ok := expectedColumns[colName]; ok {
			expectedColumns[colName] = true
		}
	}

	for col, found := range expectedColumns {
		if !found {
			t.Errorf("Expected column %s not found in compressed_data table", col)
		}
	}

	fmt.Printf("✓ compressed_data table schema correct\n")
}

func TestDataDirectory(t *testing.T) {
	// Test that data directory is created if it doesn't exist
	tmpDir := filepath.Join(os.TempDir(), "reductrai-test-new-dir", "nested", "path")
	defer os.RemoveAll(filepath.Join(os.TempDir(), "reductrai-test-new-dir"))

	db, err := NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB with new directory: %v", err)
	}
	defer db.Close()

	// Verify directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("Data directory was not created")
	}

	fmt.Printf("✓ Data directory creation working\n")
}
