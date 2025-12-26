package compression

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestSpanCompression(t *testing.T) {
	engine := NewEngineV2()

	// Generate sample spans (simulating a microservices trace)
	spans := make([]Span, 1000)
	baseTime := time.Now().UnixNano()

	services := []string{"api-gateway", "user-service", "payment-service", "order-service", "inventory-service"}
	operations := []string{"GET /api/users", "POST /api/orders", "GET /api/products", "POST /api/payments", "GET /api/inventory"}
	statuses := []string{"OK", "OK", "OK", "OK", "ERROR"}

	for i := 0; i < 1000; i++ {
		spans[i] = Span{
			TraceID:      fmt.Sprintf("trace-%d", i/10),
			SpanID:       fmt.Sprintf("span-%d", i),
			ParentSpanID: fmt.Sprintf("span-%d", i-1),
			Name:         operations[i%len(operations)],
			StartTime:    baseTime + int64(i*1000000),            // 1ms apart
			EndTime:      baseTime + int64(i*1000000) + 50000000, // 50ms duration
			Status:       statuses[i%len(statuses)],
			Service:      services[i%len(services)],
			Attributes: map[string]interface{}{
				"http.method":      "GET",
				"http.status_code": 200,
				"http.url":         "/api/v1/resource",
			},
		}
	}

	// Compress
	block, err := engine.CompressSpans(spans)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	fmt.Printf("\n=== Span Compression Test ===\n")
	fmt.Printf("Spans:            %d\n", len(spans))
	fmt.Printf("Original size:    %d bytes (%.2f KB)\n", block.OriginalSize, float64(block.OriginalSize)/1024)
	fmt.Printf("Compressed size:  %d bytes (%.2f KB)\n", block.CompressedSize, float64(block.CompressedSize)/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)
	fmt.Printf("Technique:        %s\n", block.Metadata.Technique)

	if compressionPercent < 80 {
		t.Errorf("Expected >80%% compression, got %.1f%%", compressionPercent)
	}
}

func TestLogCompression(t *testing.T) {
	engine := NewEngineV2()

	// Generate sample logs (simulating application logs)
	logs := make([]LogLine, 1000)

	levels := []string{"INFO", "DEBUG", "WARN", "ERROR", "INFO"}
	services := []string{"api-gateway", "user-service", "payment-service"}
	messages := []string{
		"Request received from client 192.168.1.100",
		"Processing order #12345 for user john@example.com",
		"Database query completed in 45ms",
		"Cache hit for key user:12345",
		"Connection established to downstream service",
	}

	for i := 0; i < 1000; i++ {
		logs[i] = LogLine{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).Format(time.RFC3339),
			Level:     levels[i%len(levels)],
			Service:   services[i%len(services)],
			Message:   messages[i%len(messages)],
			Raw:       fmt.Sprintf("%s %s [%s] %s", time.Now().Format(time.RFC3339), levels[i%len(levels)], services[i%len(services)], messages[i%len(messages)]),
		}
	}

	// Compress
	block, err := engine.CompressLogs(logs)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	fmt.Printf("\n=== Log Compression Test ===\n")
	fmt.Printf("Logs:             %d\n", len(logs))
	fmt.Printf("Original size:    %d bytes (%.2f KB)\n", block.OriginalSize, float64(block.OriginalSize)/1024)
	fmt.Printf("Compressed size:  %d bytes (%.2f KB)\n", block.CompressedSize, float64(block.CompressedSize)/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)
	fmt.Printf("Technique:        %s\n", block.Metadata.Technique)

	if compressionPercent < 80 {
		t.Errorf("Expected >80%% compression, got %.1f%%", compressionPercent)
	}
}

func TestMetricCompression(t *testing.T) {
	engine := NewEngineV2()

	// Generate sample metrics (simulating Prometheus/Datadog metrics)
	metrics := make([]MetricPoint, 1000)

	metricNames := []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"process_cpu_seconds_total",
		"go_goroutines",
		"http_response_size_bytes",
	}

	services := []string{"api-gateway", "user-service", "payment-service"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	baseTime := time.Now().UnixMilli()

	for i := 0; i < 1000; i++ {
		metrics[i] = MetricPoint{
			Metric: metricNames[i%len(metricNames)],
			Labels: map[string]string{
				"service": services[i%len(services)],
				"method":  methods[i%len(methods)],
				"status":  "200",
				"env":     "production",
			},
			Value:     float64(i%100) + 0.5,
			Timestamp: baseTime + int64(i*1000), // 1 second apart
		}
	}

	// Compress
	block, err := engine.CompressMetrics(metrics)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	fmt.Printf("\n=== Metric Compression Test ===\n")
	fmt.Printf("Metrics:          %d\n", len(metrics))
	fmt.Printf("Original size:    %d bytes (%.2f KB)\n", block.OriginalSize, float64(block.OriginalSize)/1024)
	fmt.Printf("Compressed size:  %d bytes (%.2f KB)\n", block.CompressedSize, float64(block.CompressedSize)/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)
	fmt.Printf("Technique:        %s\n", block.Metadata.Technique)

	if compressionPercent < 70 {
		t.Errorf("Expected >70%% compression, got %.1f%%", compressionPercent)
	}
}

func TestEventCompression(t *testing.T) {
	engine := NewEngineV2()

	// Generate sample events (simulating custom JSON events)
	events := make([]EventData, 1000)

	eventTypes := []string{"user.login", "user.logout", "order.created", "payment.processed", "item.viewed"}
	users := []string{"user-123", "user-456", "user-789", "user-012", "user-345"}

	for i := 0; i < 1000; i++ {
		events[i] = EventData{
			EventType: eventTypes[i%len(eventTypes)],
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).UnixMilli(),
			Properties: map[string]interface{}{
				"eventType": eventTypes[i%len(eventTypes)],
				"userId":    users[i%len(users)],
				"sessionId": fmt.Sprintf("session-%d", i/10),
				"platform":  "web",
				"country":   "US",
				"timestamp": time.Now().UnixMilli(),
			},
		}
	}

	// Compress
	block, err := engine.CompressEvents(events)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100

	fmt.Printf("\n=== Event Compression Test ===\n")
	fmt.Printf("Events:           %d\n", len(events))
	fmt.Printf("Original size:    %d bytes (%.2f KB)\n", block.OriginalSize, float64(block.OriginalSize)/1024)
	fmt.Printf("Compressed size:  %d bytes (%.2f KB)\n", block.CompressedSize, float64(block.CompressedSize)/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)
	fmt.Printf("Technique:        %s\n", block.Metadata.Technique)

	if compressionPercent < 70 {
		t.Errorf("Expected >70%% compression, got %.1f%%", compressionPercent)
	}
}

func TestAutoDetection(t *testing.T) {
	engine := NewEngineV2()

	// Test OTLP trace format auto-detection
	otlpData := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]interface{}{"stringValue": "test-service"}},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"spans": []map[string]interface{}{
							{
								"traceId":           "abc123",
								"spanId":            "def456",
								"name":              "GET /api/test",
								"startTimeUnixNano": "1700000000000000000",
								"endTimeUnixNano":   "1700000050000000000",
							},
						},
					},
				},
			},
		},
	}

	data, _ := json.Marshal(otlpData)
	block, err := engine.Compress(data, "")
	if err != nil {
		t.Fatalf("Auto-detection failed: %v", err)
	}

	fmt.Printf("\n=== Auto-Detection Test ===\n")
	fmt.Printf("Input format:     OTLP traces\n")
	fmt.Printf("Detected format:  %s\n", block.Metadata.Format)
	fmt.Printf("Technique:        %s\n", block.Metadata.Technique)

	if block.Metadata.Format != "traces" {
		t.Errorf("Expected format 'traces', got '%s'", block.Metadata.Format)
	}
}

func TestEngineInfo(t *testing.T) {
	engine := NewEngineV2()
	info := engine.GetAdapterInfo()

	fmt.Printf("\n=== Engine Info ===\n")
	fmt.Printf("Version:          %v\n", info["version"])
	fmt.Printf("Total adapters:   %v\n", info["totalAdapters"])

	if adapters, ok := info["adapters"].([]map[string]interface{}); ok {
		for _, adapter := range adapters {
			fmt.Printf("  - %s: %v\n", adapter["name"], adapter["formats"])
		}
	}
}
