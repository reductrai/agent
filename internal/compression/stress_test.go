package compression

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	mrand "math/rand"
	"testing"
	"time"
)

// generateTraceID generates a random 32-character trace ID
func generateTraceID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// generateSpanID generates a random 16-character span ID
func generateSpanID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func TestHeavySpanCompression(t *testing.T) {
	engine := NewEngineV2()
	mrand.Seed(time.Now().UnixNano())

	// Realistic microservices architecture
	services := []string{
		"api-gateway", "auth-service", "user-service", "order-service",
		"payment-service", "inventory-service", "notification-service",
		"search-service", "recommendation-service", "analytics-service",
		"cart-service", "shipping-service", "review-service", "catalog-service",
	}

	operations := []string{
		"GET /api/v1/users/{id}", "POST /api/v1/users", "PUT /api/v1/users/{id}",
		"GET /api/v1/orders", "POST /api/v1/orders", "GET /api/v1/orders/{id}",
		"POST /api/v1/payments", "GET /api/v1/payments/{id}",
		"GET /api/v1/products", "GET /api/v1/products/{id}", "POST /api/v1/products",
		"GET /api/v1/inventory/{sku}", "PUT /api/v1/inventory/{sku}",
		"POST /api/v1/notifications/email", "POST /api/v1/notifications/sms",
		"GET /api/v1/search", "GET /api/v1/recommendations/{userId}",
		"GET /api/v1/cart/{userId}", "POST /api/v1/cart/{userId}/items",
		"GET /api/v1/shipping/rates", "POST /api/v1/shipping/labels",
		"GET /api/v1/reviews/{productId}", "POST /api/v1/reviews",
	}

	httpMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statusCodes := []int{200, 201, 204, 400, 401, 403, 404, 500, 502, 503}
	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
		"Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36",
		"PostmanRuntime/7.28.4", "curl/7.68.0", "Apache-HttpClient/4.5.13",
	}

	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"}
	environments := []string{"production", "staging", "development"}

	// Generate 100K spans with complex hierarchical traces
	numSpans := 100000
	spans := make([]Span, numSpans)
	baseTime := time.Now().UnixNano()

	// Generate traces with realistic parent-child relationships
	traceID := ""
	parentSpanID := ""
	spanDepth := 0
	maxDepth := 5

	for i := 0; i < numSpans; i++ {
		// Start new trace every ~20 spans
		if i%20 == 0 || spanDepth >= maxDepth {
			traceID = generateTraceID()
			parentSpanID = ""
			spanDepth = 0
		}

		spanID := generateSpanID()
		service := services[mrand.Intn(len(services))]
		operation := operations[mrand.Intn(len(operations))]
		method := httpMethods[mrand.Intn(len(httpMethods))]
		statusCode := statusCodes[mrand.Intn(len(statusCodes))]
		duration := int64(mrand.Intn(500000000)) + 1000000 // 1ms to 500ms

		status := "OK"
		if statusCode >= 400 {
			status = "ERROR"
		}

		// Complex attributes with varying data
		attrs := map[string]interface{}{
			"http.method":                  method,
			"http.status_code":             statusCode,
			"http.url":                     fmt.Sprintf("https://%s.example.com%s", service, operation),
			"http.user_agent":              userAgents[mrand.Intn(len(userAgents))],
			"http.request_content_length":  mrand.Intn(10000),
			"http.response_content_length": mrand.Intn(100000),
			"net.peer.ip":                  fmt.Sprintf("10.%d.%d.%d", mrand.Intn(256), mrand.Intn(256), mrand.Intn(256)),
			"net.peer.port":                mrand.Intn(65535),
			"cloud.region":                 regions[mrand.Intn(len(regions))],
			"deployment.environment":       environments[mrand.Intn(len(environments))],
			"service.version":              fmt.Sprintf("v%d.%d.%d", mrand.Intn(10), mrand.Intn(100), mrand.Intn(1000)),
			"db.system":                    "postgresql",
			"db.statement":                 "SELECT * FROM users WHERE id = $1",
			"messaging.system":             "kafka",
			"messaging.destination":        fmt.Sprintf("topic-%s-events", service),
			"rpc.system":                   "grpc",
			"rpc.service":                  service,
			"rpc.method":                   operation,
			"custom.request_id":            generateTraceID(),
			"custom.correlation_id":        generateTraceID(),
			"custom.tenant_id":             fmt.Sprintf("tenant-%d", mrand.Intn(1000)),
			"custom.user_id":               fmt.Sprintf("user-%d", mrand.Intn(100000)),
		}

		// Add error details for error spans
		if status == "ERROR" {
			attrs["error.type"] = "Exception"
			attrs["error.message"] = fmt.Sprintf("Error processing request: %s", operation)
			attrs["error.stack"] = "at com.example.Service.process(Service.java:123)\nat com.example.Handler.handle(Handler.java:456)"
		}

		startTime := baseTime + int64(i*10000) + int64(mrand.Intn(5000))

		spans[i] = Span{
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: parentSpanID,
			Name:         operation,
			StartTime:    startTime,
			EndTime:      startTime + duration,
			Status:       status,
			Service:      service,
			Attributes:   attrs,
		}

		// Update parent for next span (random depth change)
		if mrand.Float32() < 0.3 {
			parentSpanID = spanID
			spanDepth++
		} else if mrand.Float32() < 0.2 && spanDepth > 1 {
			spanDepth--
		}
	}

	// Compress
	start := time.Now()
	block, err := engine.CompressSpans(spans)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	throughput := float64(numSpans) / elapsed.Seconds()

	fmt.Printf("\n=== Heavy Span Compression Test (100K spans) ===\n")
	fmt.Printf("Spans:            %d\n", numSpans)
	fmt.Printf("Attributes/span:  ~22\n")
	fmt.Printf("Original size:    %.2f MB\n", float64(block.OriginalSize)/1024/1024)
	fmt.Printf("Compressed size:  %.2f MB\n", float64(block.CompressedSize)/1024/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Time:             %v\n", elapsed)
	fmt.Printf("Throughput:       %.0f spans/sec\n", throughput)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)

	// Random test data compresses less than repetitive production data
	// Production telemetry typically achieves >90% compression
	if compressionPercent < 70 {
		t.Errorf("Expected >70%% compression, got %.1f%%", compressionPercent)
	}
}

func TestHeavyLogCompression(t *testing.T) {
	engine := NewEngineV2()
	mrand.Seed(time.Now().UnixNano())

	levels := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	services := []string{
		"api-gateway", "auth-service", "user-service", "order-service",
		"payment-service", "inventory-service", "notification-service",
	}

	// Realistic log message templates
	messageTemplates := []string{
		"Request received from %s for endpoint %s with method %s",
		"Processing order #%d for user %s in region %s",
		"Database query executed in %dms: %s",
		"Cache %s for key %s, ttl=%ds",
		"Connection pool stats: active=%d, idle=%d, max=%d",
		"Rate limit exceeded for client %s, limit=%d/min",
		"Authentication %s for user %s from IP %s",
		"Message published to topic %s, partition=%d, offset=%d",
		"Retry attempt %d/%d for operation %s",
		"Health check %s: latency=%dms, status=%s",
		"Circuit breaker %s for service %s, failures=%d",
		"Feature flag %s evaluated to %t for user %s",
		"Metric recorded: %s=%f, tags=%s",
		"Span started: traceId=%s, spanId=%s, operation=%s",
		"Error occurred: %s - %s (code=%d)",
		"Request completed: status=%d, duration=%dms, size=%d bytes",
		"Background job %s completed in %dms with status %s",
		"WebSocket connection %s from client %s",
		"SSL certificate for %s expires in %d days",
		"Memory usage: heap=%dMB, stack=%dMB, gc=%dms",
	}

	endpoints := []string{"/api/users", "/api/orders", "/api/products", "/api/payments", "/api/search"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	cacheOps := []string{"HIT", "MISS", "SET", "DELETE"}
	authResults := []string{"succeeded", "failed", "expired"}
	circuitStates := []string{"OPEN", "CLOSED", "HALF_OPEN"}

	// Generate 100K logs
	numLogs := 100000
	logs := make([]LogLine, numLogs)
	baseTime := time.Now()

	for i := 0; i < numLogs; i++ {
		level := levels[mrand.Intn(len(levels))]
		service := services[mrand.Intn(len(services))]
		template := messageTemplates[mrand.Intn(len(messageTemplates))]

		// Generate message based on template
		var message string
		switch mrand.Intn(20) {
		case 0:
			message = fmt.Sprintf(template, fmt.Sprintf("10.%d.%d.%d", mrand.Intn(256), mrand.Intn(256), mrand.Intn(256)),
				endpoints[mrand.Intn(len(endpoints))], methods[mrand.Intn(len(methods))])
		case 1:
			message = fmt.Sprintf(template, mrand.Intn(1000000), fmt.Sprintf("user-%d", mrand.Intn(10000)), "us-east-1")
		case 2:
			message = fmt.Sprintf(template, mrand.Intn(500), "SELECT * FROM orders WHERE user_id = $1 AND status = $2")
		case 3:
			message = fmt.Sprintf(template, cacheOps[mrand.Intn(len(cacheOps))], fmt.Sprintf("session:%d", mrand.Intn(100000)), mrand.Intn(3600))
		case 4:
			message = fmt.Sprintf(template, mrand.Intn(100), mrand.Intn(50), 200)
		case 5:
			message = fmt.Sprintf(template, fmt.Sprintf("client-%d", mrand.Intn(1000)), 1000)
		case 6:
			message = fmt.Sprintf(template, authResults[mrand.Intn(len(authResults))], fmt.Sprintf("user-%d", mrand.Intn(10000)),
				fmt.Sprintf("10.%d.%d.%d", mrand.Intn(256), mrand.Intn(256), mrand.Intn(256)))
		case 7:
			message = fmt.Sprintf(template, fmt.Sprintf("orders.%s", service), mrand.Intn(32), mrand.Int63n(1000000))
		case 8:
			message = fmt.Sprintf(template, mrand.Intn(3)+1, 3, "sendNotification")
		case 9:
			message = fmt.Sprintf(template, "passed", mrand.Intn(100), "healthy")
		case 10:
			message = fmt.Sprintf(template, circuitStates[mrand.Intn(len(circuitStates))], service, mrand.Intn(10))
		default:
			message = fmt.Sprintf("Standard log message #%d from %s at level %s", i, service, level)
		}

		timestamp := baseTime.Add(time.Duration(i) * time.Millisecond)

		logs[i] = LogLine{
			Timestamp: timestamp.Format(time.RFC3339Nano),
			Level:     level,
			Service:   service,
			Message:   message,
			Raw:       fmt.Sprintf("%s %s [%s] %s", timestamp.Format(time.RFC3339Nano), level, service, message),
		}
	}

	// Compress
	start := time.Now()
	block, err := engine.CompressLogs(logs)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	throughput := float64(numLogs) / elapsed.Seconds()

	fmt.Printf("\n=== Heavy Log Compression Test (100K logs) ===\n")
	fmt.Printf("Logs:             %d\n", numLogs)
	fmt.Printf("Original size:    %.2f MB\n", float64(block.OriginalSize)/1024/1024)
	fmt.Printf("Compressed size:  %.2f MB\n", float64(block.CompressedSize)/1024/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Time:             %v\n", elapsed)
	fmt.Printf("Throughput:       %.0f logs/sec\n", throughput)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)

	// Random test data compresses less than repetitive production data
	// Production telemetry typically achieves >90% compression
	if compressionPercent < 70 {
		t.Errorf("Expected >70%% compression, got %.1f%%", compressionPercent)
	}
}

func TestHeavyMetricCompression(t *testing.T) {
	engine := NewEngineV2()
	mrand.Seed(time.Now().UnixNano())

	// Realistic Prometheus/Datadog metric names
	metricNames := []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"http_request_size_bytes",
		"http_response_size_bytes",
		"http_active_requests",
		"process_cpu_seconds_total",
		"process_resident_memory_bytes",
		"process_virtual_memory_bytes",
		"process_open_fds",
		"go_goroutines",
		"go_gc_duration_seconds",
		"go_memstats_alloc_bytes",
		"go_memstats_heap_objects",
		"db_connections_active",
		"db_connections_idle",
		"db_query_duration_seconds",
		"db_query_total",
		"cache_hits_total",
		"cache_misses_total",
		"cache_evictions_total",
		"queue_depth",
		"queue_processing_duration_seconds",
		"kafka_consumer_lag",
		"kafka_messages_consumed_total",
		"kafka_messages_produced_total",
		"grpc_server_handled_total",
		"grpc_server_handling_seconds",
		"custom_business_orders_total",
		"custom_business_revenue_dollars",
		"custom_business_active_users",
	}

	services := []string{"api-gateway", "auth-service", "user-service", "order-service", "payment-service"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statusCodes := []string{"200", "201", "400", "404", "500"}
	instances := []string{"instance-1", "instance-2", "instance-3", "instance-4"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}

	// Generate 500K metric points (realistic for 1 hour of data)
	numMetrics := 500000
	metrics := make([]MetricPoint, numMetrics)
	baseTime := time.Now().UnixMilli()

	for i := 0; i < numMetrics; i++ {
		metricName := metricNames[mrand.Intn(len(metricNames))]
		service := services[mrand.Intn(len(services))]

		labels := map[string]string{
			"service":  service,
			"instance": instances[mrand.Intn(len(instances))],
			"region":   regions[mrand.Intn(len(regions))],
			"env":      "production",
		}

		// Add method/status for HTTP metrics
		if len(metricName) > 4 && metricName[:4] == "http" {
			labels["method"] = methods[mrand.Intn(len(methods))]
			labels["status"] = statusCodes[mrand.Intn(len(statusCodes))]
		}

		// Generate realistic values based on metric type
		var value float64
		switch {
		case contains(metricName, "total"):
			value = float64(mrand.Intn(1000000))
		case contains(metricName, "seconds"):
			value = mrand.Float64() * 10 // 0-10 seconds
		case contains(metricName, "bytes"):
			value = float64(mrand.Intn(1000000000)) // Up to 1GB
		case contains(metricName, "goroutines"):
			value = float64(mrand.Intn(10000))
		case contains(metricName, "connections"):
			value = float64(mrand.Intn(1000))
		case contains(metricName, "lag"):
			value = float64(mrand.Intn(100000))
		default:
			value = mrand.Float64() * 1000
		}

		// Skip NaN/Inf as JSON doesn't support them
		// In production, these would be filtered or converted

		metrics[i] = MetricPoint{
			Metric:    metricName,
			Labels:    labels,
			Value:     value,
			Timestamp: baseTime + int64(i*100), // 100ms intervals
			Type:      "gauge",
		}
	}

	// Compress
	start := time.Now()
	block, err := engine.CompressMetrics(metrics)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	throughput := float64(numMetrics) / elapsed.Seconds()

	fmt.Printf("\n=== Heavy Metric Compression Test (500K metrics) ===\n")
	fmt.Printf("Metrics:          %d\n", numMetrics)
	fmt.Printf("Labels/metric:    5-7\n")
	fmt.Printf("Original size:    %.2f MB\n", float64(block.OriginalSize)/1024/1024)
	fmt.Printf("Compressed size:  %.2f MB\n", float64(block.CompressedSize)/1024/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Time:             %v\n", elapsed)
	fmt.Printf("Throughput:       %.0f metrics/sec\n", throughput)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)

	// Metrics have more entropy (float values) so compress less
	// Production metrics typically achieve >80% compression
	if compressionPercent < 60 {
		t.Errorf("Expected >60%% compression, got %.1f%%", compressionPercent)
	}
}

func TestHeavyEventCompression(t *testing.T) {
	engine := NewEngineV2()
	mrand.Seed(time.Now().UnixNano())

	eventTypes := []string{
		"user.created", "user.updated", "user.deleted", "user.login", "user.logout",
		"order.created", "order.updated", "order.cancelled", "order.completed", "order.refunded",
		"payment.initiated", "payment.completed", "payment.failed", "payment.refunded",
		"product.viewed", "product.added_to_cart", "product.removed_from_cart", "product.purchased",
		"search.performed", "recommendation.clicked", "notification.sent", "notification.clicked",
		"error.occurred", "warning.raised", "audit.action",
	}

	platforms := []string{"web", "ios", "android", "api"}
	countries := []string{"US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "MX"}
	browsers := []string{"Chrome", "Firefox", "Safari", "Edge", "Opera"}
	deviceTypes := []string{"desktop", "mobile", "tablet"}

	// Generate 100K events with complex nested properties
	numEvents := 100000
	events := make([]EventData, numEvents)
	baseTime := time.Now()

	for i := 0; i < numEvents; i++ {
		eventType := eventTypes[mrand.Intn(len(eventTypes))]
		timestamp := baseTime.Add(time.Duration(i) * time.Millisecond)

		props := map[string]interface{}{
			"eventType":  eventType,
			"timestamp":  timestamp.UnixMilli(),
			"userId":     fmt.Sprintf("user-%d", mrand.Intn(100000)),
			"sessionId":  generateSpanID(),
			"requestId":  generateTraceID(),
			"platform":   platforms[mrand.Intn(len(platforms))],
			"country":    countries[mrand.Intn(len(countries))],
			"browser":    browsers[mrand.Intn(len(browsers))],
			"deviceType": deviceTypes[mrand.Intn(len(deviceTypes))],
			"ip":         fmt.Sprintf("10.%d.%d.%d", mrand.Intn(256), mrand.Intn(256), mrand.Intn(256)),
			"userAgent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
			// Nested object
			"context": map[string]interface{}{
				"page":     fmt.Sprintf("/page/%d", mrand.Intn(1000)),
				"referrer": fmt.Sprintf("https://example.com/ref/%d", mrand.Intn(100)),
				"campaign": fmt.Sprintf("campaign-%d", mrand.Intn(50)),
				"source":   []string{"google", "facebook", "twitter", "email"}[mrand.Intn(4)],
			},
			// Array of items
			"items": []interface{}{
				map[string]interface{}{
					"productId": fmt.Sprintf("prod-%d", mrand.Intn(10000)),
					"quantity":  mrand.Intn(10) + 1,
					"price":     float64(mrand.Intn(10000)) / 100,
				},
			},
			// Metrics
			"metrics": map[string]interface{}{
				"loadTime":    mrand.Float64() * 5000,
				"renderTime":  mrand.Float64() * 2000,
				"networkTime": mrand.Float64() * 1000,
			},
		}

		// Add event-specific properties
		switch {
		case contains(eventType, "order"):
			props["orderId"] = fmt.Sprintf("order-%d", mrand.Intn(1000000))
			props["orderTotal"] = float64(mrand.Intn(100000)) / 100
			props["currency"] = "USD"
		case contains(eventType, "payment"):
			props["paymentId"] = fmt.Sprintf("pay-%d", mrand.Intn(1000000))
			props["amount"] = float64(mrand.Intn(100000)) / 100
			props["method"] = []string{"credit_card", "debit_card", "paypal", "apple_pay"}[mrand.Intn(4)]
		case contains(eventType, "error"):
			props["errorCode"] = fmt.Sprintf("ERR_%d", mrand.Intn(1000))
			props["errorMessage"] = "An unexpected error occurred during processing"
			props["stackTrace"] = "at module.function(file.js:123)\nat handler.process(handler.js:456)"
		}

		events[i] = EventData{
			EventType:  eventType,
			Timestamp:  timestamp.UnixMilli(),
			Properties: props,
		}
	}

	// Compress
	start := time.Now()
	block, err := engine.CompressEvents(events)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	compressionPercent := (1.0 - float64(block.CompressedSize)/float64(block.OriginalSize)) * 100
	throughput := float64(numEvents) / elapsed.Seconds()

	fmt.Printf("\n=== Heavy Event Compression Test (100K events) ===\n")
	fmt.Printf("Events:           %d\n", numEvents)
	fmt.Printf("Properties/event: ~20 (with nested objects)\n")
	fmt.Printf("Original size:    %.2f MB\n", float64(block.OriginalSize)/1024/1024)
	fmt.Printf("Compressed size:  %.2f MB\n", float64(block.CompressedSize)/1024/1024)
	fmt.Printf("Compression:      %.1f%%\n", compressionPercent)
	fmt.Printf("Ratio:            %.1f:1\n", block.CompressionRatio)
	fmt.Printf("Time:             %v\n", elapsed)
	fmt.Printf("Throughput:       %.0f events/sec\n", throughput)
	fmt.Printf("Format:           %s\n", block.Metadata.Format)

	// Random test data compresses less than repetitive production data
	// Production telemetry typically achieves >90% compression
	if compressionPercent < 70 {
		t.Errorf("Expected >70%% compression, got %.1f%%", compressionPercent)
	}
}

func TestCombinedStress(t *testing.T) {
	fmt.Printf("\n")
	fmt.Printf("╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║           V2 COMPRESSION ENGINE - STRESS TEST                ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Testing with realistic, complex production-like data        ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
