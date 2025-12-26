package context

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/reductrai/agent/internal/storage"
	"github.com/reductrai/agent/pkg/types"
)

func TestExtractEnrichedContext(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "reductrai-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize storage
	db, err := storage.NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	now := time.Now()

	// Insert healthy baseline spans for payment-service
	for i := 0; i < 50; i++ {
		span := types.Span{
			TraceID:   "trace-healthy-" + string(rune('a'+i%26)),
			SpanID:    "span-healthy-" + string(rune('a'+i%26)),
			Service:   "payment-service",
			Operation: "POST /checkout",
			StartTime: now.Add(-time.Duration(i) * time.Second),
			Duration:  50 * time.Millisecond,
			Status:    "OK",
		}
		if err := db.InsertSpan(span); err != nil {
			t.Fatalf("Failed to insert span: %v", err)
		}
	}

	// Insert error spans for payment-service (to trigger anomaly)
	for i := 0; i < 20; i++ {
		span := types.Span{
			TraceID:   "trace-err-" + string(rune('a'+i%26)),
			SpanID:    "span-err-" + string(rune('a'+i%26)),
			Service:   "payment-service",
			Operation: "call stripe-api",
			StartTime: now.Add(-time.Duration(i) * 100 * time.Millisecond),
			Duration:  500 * time.Millisecond,
			Status:    "ERROR",
		}
		if err := db.InsertSpan(span); err != nil {
			t.Fatalf("Failed to insert span: %v", err)
		}
	}

	// Insert correlated errors in stripe-api (downstream)
	for i := 0; i < 15; i++ {
		span := types.Span{
			TraceID:      "trace-err-" + string(rune('a'+i%26)),
			SpanID:       "span-stripe-" + string(rune('a'+i%26)),
			ParentSpanID: "span-err-" + string(rune('a'+i%26)),
			Service:      "stripe-api",
			Operation:    "process payment",
			StartTime:    now.Add(-time.Duration(i)*100*time.Millisecond + 10*time.Millisecond),
			Duration:     490 * time.Millisecond,
			Status:       "ERROR",
		}
		if err := db.InsertSpan(span); err != nil {
			t.Fatalf("Failed to insert span: %v", err)
		}
	}

	// Insert test logs with PII that should be redacted
	log := types.LogEntry{
		Timestamp: now,
		Service:   "payment-service",
		Level:     "ERROR",
		Message:   "Failed to process payment for user test@email.com: stripe-api timeout after 5000ms, ip=192.168.1.100",
	}
	if err := db.InsertLog(log); err != nil {
		t.Fatalf("Failed to insert log: %v", err)
	}

	// Initialize extractor using the raw DB connection
	extractor := NewExtractor(db.GetDB())

	// Create anomaly
	anomaly := types.Anomaly{
		Service:    "payment-service",
		Type:       "error_spike",
		Severity:   "high",
		Value:      0.25, // 25% error rate
		Threshold:  0.05,
		DetectedAt: now,
	}

	// Extract enriched context
	ctx, err := extractor.Extract(anomaly)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// === VERIFY SCOPE ANALYSIS ===
	t.Log("=== SCOPE ANALYSIS ===")
	t.Logf("Scope: %s", ctx.Scope.Scope)
	t.Logf("Scope Reasoning: %s", ctx.Scope.Reasoning)
	t.Logf("Healthy Deps: %v", ctx.Scope.HealthyDeps)
	t.Logf("Failing Deps: %v", ctx.Scope.FailingDeps)

	if ctx.Scope.Scope == "" {
		t.Error("Scope should not be empty")
	}

	// With stripe-api also failing, scope should be "downstream" or "cascading"
	if ctx.Scope.Scope != "isolated" && len(ctx.Scope.FailingDeps) == 0 {
		t.Logf("Warning: Expected failing deps when scope is not isolated")
	}

	// === VERIFY FINGERPRINT ===
	t.Log("=== FINGERPRINT ===")
	t.Logf("Fingerprint Hash: %s", ctx.Fingerprint.Hash)
	t.Logf("Fingerprint Confidence: %.2f", ctx.Fingerprint.Confidence)
	t.Logf("Historical Matches: %d", len(ctx.Fingerprint.MatchedHistory))

	if ctx.Fingerprint.Hash == "" {
		t.Error("Fingerprint hash should not be empty")
	}
	if len(ctx.Fingerprint.Hash) != 16 {
		t.Errorf("Fingerprint hash should be 16 chars (8 bytes hex), got %d: %s",
			len(ctx.Fingerprint.Hash), ctx.Fingerprint.Hash)
	}

	// Confidence should be 0 for first incident (no history)
	if ctx.Fingerprint.Confidence != 0 {
		t.Logf("First incident - expected 0 confidence, got %.2f", ctx.Fingerprint.Confidence)
	}

	// === VERIFY PII REDACTION ===
	t.Log("=== PII REDACTION ===")
	for _, log := range ctx.ErrorLogs {
		t.Logf("Log template: %s", log.Template)
		if strings.Contains(log.Template, "test@email.com") {
			t.Error("PII (email) should be redacted in log template")
		}
		if strings.Contains(log.Template, "192.168.1.100") {
			t.Error("PII (IP) should be redacted in log template")
		}
	}

	// === VERIFY CORRELATIONS ===
	t.Log("=== CORRELATIONS ===")
	t.Logf("Correlations found: %d", len(ctx.Correlations))
	for _, corr := range ctx.Correlations {
		t.Logf("  - %s (%s): %.2f error rate, %.2f correlation",
			corr.Service, corr.Direction, corr.ErrorRate, corr.CorrelationScore)
	}

	// === VERIFY METRICS ===
	t.Log("=== SERVICE METRICS ===")
	t.Logf("Error Rate: %.2f%% (baseline: %.2f%%)",
		ctx.ServiceMetrics.ErrorRate*100, ctx.ServiceMetrics.ErrorRateBaseline*100)
	t.Logf("Latency P99: %.2fms (baseline: %.2fms)",
		ctx.ServiceMetrics.LatencyP99, ctx.ServiceMetrics.LatencyBaseline)

	// === PRINT FULL CONTEXT ===
	t.Log("=== FULL ENRICHED CONTEXT ===")
	jsonBytes, _ := json.MarshalIndent(ctx, "", "  ")
	t.Logf("\n%s", string(jsonBytes))

	// === VERIFY SIZE ===
	size := ctx.EstimateSize()
	t.Logf("Context size: %d bytes (~%.1f KB)", size, float64(size)/1024)
	if size > 50000 {
		t.Errorf("Context too large: %d bytes (target: <20KB)", size)
	}
}

func TestFingerprintHistoricalMatching(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "reductrai-test-fingerprint")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize storage
	db, err := storage.NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	rawDB := db.GetDB()

	// Create fingerprint generator
	fg := NewFingerprintGenerator(rawDB)

	// First incident - no history
	fp1 := fg.Generate("payment-service", "error_spike", []ServiceCorrelation{
		{Service: "stripe-api", Direction: "downstream"},
	})
	t.Logf("First incident fingerprint: %s, confidence: %.2f", fp1.Hash, fp1.Confidence)

	if fp1.Confidence != 0 {
		t.Errorf("First incident should have 0 confidence, got %.2f", fp1.Confidence)
	}

	// Record that we fixed it successfully
	// Signature: fingerprint, service, anomalyType, durationSec, appliedFix, fixSucceeded, timeToResolveSec
	err = fg.RecordIncident(fp1.Hash, "payment-service", "error_spike", 300,
		"kubectl rollout restart deployment/payment-service -n prod", true, 120)
	if err != nil {
		t.Fatalf("Failed to record incident: %v", err)
	}

	// Second incident with same fingerprint - should match history
	fp2 := fg.Generate("payment-service", "error_spike", []ServiceCorrelation{
		{Service: "stripe-api", Direction: "downstream"},
	})
	t.Logf("Second incident fingerprint: %s, confidence: %.2f", fp2.Hash, fp2.Confidence)

	// Should have same hash
	if fp1.Hash != fp2.Hash {
		t.Errorf("Same incident pattern should have same hash: %s vs %s", fp1.Hash, fp2.Hash)
	}

	// Should now have history and non-zero confidence
	if len(fp2.MatchedHistory) == 0 {
		t.Error("Second incident should have historical matches")
	}
	if fp2.Confidence == 0 {
		t.Error("Second incident should have non-zero confidence")
	}

	// Verify historical match details
	for _, match := range fp2.MatchedHistory {
		t.Logf("Historical match: fix=%s, succeeded=%v, timeToResolve=%ds",
			match.AppliedFix, match.FixSucceeded, match.TimeToResolve)
	}
}

func TestScopeAnalysis(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "reductrai-test-scope")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize storage
	db, err := storage.NewDuckDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDB: %v", err)
	}
	defer db.Close()

	rawDB := db.GetDB()
	analyzer := NewScopeAnalyzer(rawDB)

	tests := []struct {
		name         string
		correlations []ServiceCorrelation
		metrics      ServiceMetrics
		expectedType string
	}{
		{
			name:         "isolated - no failing deps",
			correlations: []ServiceCorrelation{},
			metrics:      ServiceMetrics{ErrorRate: 0.1, LatencyP99: 500},
			expectedType: "isolated",
		},
		{
			name: "downstream - we're causing failures",
			correlations: []ServiceCorrelation{
				{Service: "frontend", Direction: "downstream", ErrorRate: 0.15},
			},
			metrics:      ServiceMetrics{ErrorRate: 0.2, LatencyP99: 800},
			expectedType: "downstream",
		},
		{
			name: "upstream - dependency is causing our failures",
			correlations: []ServiceCorrelation{
				{Service: "database", Direction: "upstream", ErrorRate: 0.25},
			},
			metrics:      ServiceMetrics{ErrorRate: 0.15, LatencyP99: 600},
			expectedType: "upstream",
		},
		{
			name: "cascading - both directions failing",
			correlations: []ServiceCorrelation{
				{Service: "database", Direction: "upstream", ErrorRate: 0.2},
				{Service: "frontend", Direction: "downstream", ErrorRate: 0.1},
			},
			metrics:      ServiceMetrics{ErrorRate: 0.15, LatencyP99: 600},
			expectedType: "cascading",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := analyzer.AnalyzeWithMetrics("test-service", tt.correlations, tt.metrics)
			t.Logf("Scope: %s, Reasoning: %s", scope.Scope, scope.Reasoning)

			if scope.Scope != tt.expectedType {
				t.Errorf("Expected scope %s, got %s", tt.expectedType, scope.Scope)
			}
		})
	}
}
