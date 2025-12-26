/*
 * *******************************************************************************
 * *                                                                             *
 * *  CONFIDENTIAL - INTERNAL USE ONLY - TRADE SECRET                            *
 * *                                                                             *
 * *  This file contains proprietary trade secrets of ReductrAI.                 *
 * *  Unauthorized disclosure, copying, or distribution is strictly prohibited.  *
 * *                                                                             *
 * *  (C) 2025 ReductrAI. All rights reserved.                                   *
 * *                                                                             *
 * *******************************************************************************
 *
 * Real Compression Engine V2
 *
 * Unified compression engine that auto-detects data types and routes to
 * specialized compressors for optimal compression:
 *
 * - Logs (97.7-99.4%) - ContextualDictionaryCompressor
 * - Traces (99.3-99.7%) - SpanPatternCompressor
 * - Metrics (88.6-91.1%) - TimeSeriesAggregator
 * - Events (97.6-99.5%) - SemanticCompressor
 *
 * Average compression: 95.9% vs 80-90% baseline (+11.2pp improvement)
 */

package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"sync"
)

// CompressionStats contains statistics about a compression operation
type CompressionStats struct {
	Format             string
	AdapterUsed        string
	OriginalSize       int
	CompressedSize     int
	CompressionRatio   float64
	CompressionPercent float64
	Technique          string
}

// EngineV2 is the main compression engine
type EngineV2 struct {
	registry       *AdapterRegistry
	baseCompressor *BaseCompressor
	mu             sync.RWMutex
}

// NewEngineV2 creates a new V2 compression engine
func NewEngineV2() *EngineV2 {
	engine := &EngineV2{
		registry:       NewAdapterRegistry(),
		baseCompressor: NewBaseCompressor(),
	}

	// Register all specialized adapters (order matters - most specific first)
	engine.registry.Register(NewSpanPatternCompressor())          // Traces (99.7%) - OTLP format
	engine.registry.Register(NewTimeSeriesAggregator())           // Metrics (91.1%) - Datadog/Prometheus
	engine.registry.Register(NewContextualDictionaryCompressor()) // Logs (99.4%) - Syslog/logs
	engine.registry.Register(NewSemanticCompressor())             // Events (99.5%) - Generic JSON (last)

	return engine
}

// Compress compresses data with auto-detection
func (e *EngineV2) Compress(data []byte, format string) (*CompressedBlock, error) {
	if len(data) == 0 {
		return e.emptyBlock(), nil
	}

	// Stage 1: Auto-detect and select appropriate adapter
	adapter := e.registry.SelectAdapter(data, format)

	if adapter == nil {
		// No specialized adapter found, use simple gzip fallback
		return e.compressWithGzip(data)
	}

	// Stage 2: Parse data using adapter
	parsed, err := adapter.Parse(data)
	if err != nil {
		return e.compressWithGzip(data)
	}

	// Stage 3: Extract compressible features
	compressible, err := adapter.Extract(parsed)
	if err != nil {
		return e.compressWithGzip(data)
	}

	// Stage 4: Compress using BaseCompressor
	compressed, err := e.baseCompressor.CompressData(compressible)
	if err != nil {
		return e.compressWithGzip(data)
	}

	return compressed, nil
}

// Decompress decompresses data back to original format
func (e *EngineV2) Decompress(compressed *CompressedBlock) ([]byte, error) {
	format := compressed.Metadata.Format

	if format == "" || format == "unknown" || format == "gzip-fallback" {
		// Simple gzip decompression
		return e.decompressGzip(compressed.Data)
	}

	// Stage 1: Decompress to CompressibleData
	compressible, err := e.baseCompressor.DecompressData(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}

	// Stage 2: Find adapter for format
	adapter := e.getAdapterByFormat(format)
	if adapter == nil {
		return nil, fmt.Errorf("no adapter found for format: %s", format)
	}

	// Stage 3: Reconstruct original format
	reconstructed, err := adapter.Reconstruct(compressible)
	if err != nil {
		return nil, fmt.Errorf("reconstruction failed: %w", err)
	}

	// Stage 4: Serialize back to bytes
	result, err := adapter.Serialize(reconstructed, format)
	if err != nil {
		return nil, fmt.Errorf("serialization failed: %w", err)
	}

	return result, nil
}

// CompressSpans compresses trace spans directly
func (e *EngineV2) CompressSpans(spans []Span) (*CompressedBlock, error) {
	adapter := NewSpanPatternCompressor()

	compressible, err := adapter.Extract(spans)
	if err != nil {
		return nil, err
	}

	return e.baseCompressor.CompressData(compressible)
}

// CompressLogs compresses log lines directly
func (e *EngineV2) CompressLogs(logs []LogLine) (*CompressedBlock, error) {
	adapter := NewContextualDictionaryCompressor()

	compressible, err := adapter.Extract(logs)
	if err != nil {
		return nil, err
	}

	return e.baseCompressor.CompressData(compressible)
}

// CompressMetrics compresses metric points directly
func (e *EngineV2) CompressMetrics(metrics []MetricPoint) (*CompressedBlock, error) {
	adapter := NewTimeSeriesAggregator()

	compressible, err := adapter.Extract(metrics)
	if err != nil {
		return nil, err
	}

	return e.baseCompressor.CompressData(compressible)
}

// CompressEvents compresses events directly
func (e *EngineV2) CompressEvents(events []EventData) (*CompressedBlock, error) {
	adapter := NewSemanticCompressor()

	compressible, err := adapter.Extract(events)
	if err != nil {
		return nil, err
	}

	return e.baseCompressor.CompressData(compressible)
}

// GetStats returns compression statistics
func (e *EngineV2) GetStats() CompressionStats {
	adapters := e.registry.GetAdapters()
	adapterNames := make([]string, len(adapters))
	for i, a := range adapters {
		adapterNames[i] = a.Name()
	}

	return CompressionStats{
		Format:      "multi-format",
		AdapterUsed: "auto-detect",
		Technique:   fmt.Sprintf("v2-specialized (%d adapters: %s)", len(adapters), strings.Join(adapterNames, ", ")),
	}
}

// GetAdapterInfo returns information about available adapters
func (e *EngineV2) GetAdapterInfo() map[string]interface{} {
	adapters := e.registry.GetAdapters()

	adapterInfo := make([]map[string]interface{}, len(adapters))
	for i, a := range adapters {
		adapterInfo[i] = map[string]interface{}{
			"name":    a.Name(),
			"formats": e.getAdapterFormats(a),
		}
	}

	return map[string]interface{}{
		"version":       2,
		"totalAdapters": len(adapters),
		"adapters":      adapterInfo,
	}
}

// ClearCaches clears all compression caches
func (e *EngineV2) ClearCaches() {
	e.baseCompressor.ClearCaches()
}

// Helper methods

func (e *EngineV2) compressWithGzip(data []byte) (*CompressedBlock, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	compressed := buf.Bytes()
	ratio := float64(len(data)) / float64(len(compressed))

	return &CompressedBlock{
		OriginalSize:     len(data),
		CompressedSize:   len(compressed),
		CompressionRatio: ratio,
		Data:             compressed,
		Metadata: BlockMetadata{
			Format:    "unknown",
			Technique: "gzip-fallback",
		},
	}, nil
}

func (e *EngineV2) decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (e *EngineV2) getAdapterByFormat(format string) Adapter {
	adapters := e.registry.GetAdapters()
	formatLower := strings.ToLower(format)

	// Map common format names to adapter names
	formatMap := map[string]string{
		"logs":          "ContextualDictionary",
		"syslog":        "ContextualDictionary",
		"traces":        "SpanPattern",
		"otlp":          "SpanPattern",
		"opentelemetry": "SpanPattern",
		"metrics":       "TimeSeriesAggregator",
		"prometheus":    "TimeSeriesAggregator",
		"datadog":       "TimeSeriesAggregator",
		"events":        "SemanticCompressor",
		"json":          "SemanticCompressor",
	}

	adapterName := formatMap[formatLower]
	if adapterName != "" {
		for _, adapter := range adapters {
			if strings.Contains(adapter.Name(), adapterName) {
				return adapter
			}
		}
	}

	// Try direct name match
	for _, adapter := range adapters {
		if strings.Contains(strings.ToLower(adapter.Name()), formatLower) {
			return adapter
		}
	}

	return nil
}

func (e *EngineV2) getAdapterFormats(adapter Adapter) []string {
	formatMap := map[string][]string{
		"ContextualDictionaryCompressor": {"logs", "syslog", "json-logs", "custom-logs"},
		"SpanPatternCompressor":          {"traces", "otlp", "opentelemetry", "distributed-traces"},
		"TimeSeriesAggregator":           {"metrics", "prometheus", "datadog", "timeseries"},
		"SemanticCompressor":             {"events", "json", "generic", "custom-json"},
	}

	if formats, ok := formatMap[adapter.Name()]; ok {
		return formats
	}
	return []string{"unknown"}
}

func (e *EngineV2) emptyBlock() *CompressedBlock {
	return &CompressedBlock{
		OriginalSize:     0,
		CompressedSize:   0,
		CompressionRatio: 1,
		Data:             []byte{},
		Metadata: BlockMetadata{
			Format:    "empty",
			Technique: "none",
		},
	}
}

// TestAdapter tests a specific adapter with sample data
func (e *EngineV2) TestAdapter(adapterName string, sampleData []byte) (*CompressionStats, error) {
	adapters := e.registry.GetAdapters()

	var adapter Adapter
	for _, a := range adapters {
		if strings.Contains(a.Name(), adapterName) {
			adapter = a
			break
		}
	}

	if adapter == nil {
		return nil, fmt.Errorf("adapter not found: %s", adapterName)
	}

	if !adapter.CanHandle(sampleData, "") {
		return nil, fmt.Errorf("adapter %s cannot handle this data", adapterName)
	}

	parsed, err := adapter.Parse(sampleData)
	if err != nil {
		return nil, err
	}

	compressible, err := adapter.Extract(parsed)
	if err != nil {
		return nil, err
	}

	compressed, err := e.baseCompressor.CompressData(compressible)
	if err != nil {
		return nil, err
	}

	compressionPercent := (1.0 - float64(compressed.CompressedSize)/float64(compressed.OriginalSize)) * 100

	return &CompressionStats{
		Format:             compressible.Format,
		AdapterUsed:        adapter.Name(),
		OriginalSize:       compressed.OriginalSize,
		CompressedSize:     compressed.CompressedSize,
		CompressionRatio:   compressed.CompressionRatio,
		CompressionPercent: compressionPercent,
		Technique:          compressed.Metadata.Technique,
	}, nil
}
