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
 * BaseCompressor
 *
 * Core compression logic shared by all adapters:
 * - LRU caching for dictionaries
 * - Brotli compression with dictionary-based substitution
 * - Delta encoding for timestamps
 * - Columnar transformation for efficient compression
 */

package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
)

const (
	// Cache configuration
	MaxDictionarySize = 1000 // Maximum entries in dictionary
	MaxPatternSize    = 500  // Maximum patterns to track
	MaxCacheSize      = 100  // Maximum cached items per type

	// Compression levels
	CompressionLevelFast    = 1
	CompressionLevelDefault = 6
	CompressionLevelBest    = 9
)

// LRUCache is a simple LRU cache implementation
type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]interface{}
	order    []string
}

// NewLRUCache creates a new LRU cache
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]interface{}),
		order:    make([]string, 0, capacity),
	}
}

// Get retrieves an item from the cache
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	return item, exists
}

// Set adds an item to the cache
func (c *LRUCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If key exists, move to front
	if _, exists := c.items[key]; exists {
		c.moveToFront(key)
		c.items[key] = value
		return
	}

	// Evict if at capacity
	if len(c.items) >= c.capacity {
		oldest := c.order[0]
		delete(c.items, oldest)
		c.order = c.order[1:]
	}

	c.items[key] = value
	c.order = append(c.order, key)
}

// Clear removes all items from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]interface{})
	c.order = make([]string, 0, c.capacity)
}

func (c *LRUCache) moveToFront(key string) {
	// Find and remove from current position
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	c.order = append(c.order, key)
}

// BaseCompressor handles core compression logic
type BaseCompressor struct {
	dictionaryCache *LRUCache
	patternCache    *LRUCache
	level           int
}

// NewBaseCompressor creates a new base compressor
func NewBaseCompressor() *BaseCompressor {
	return &BaseCompressor{
		dictionaryCache: NewLRUCache(MaxCacheSize),
		patternCache:    NewLRUCache(MaxCacheSize),
		level:           CompressionLevelDefault,
	}
}

// SetCompressionLevel sets the compression level (1-9)
func (b *BaseCompressor) SetCompressionLevel(level int) {
	if level < 1 {
		level = 1
	}
	if level > 9 {
		level = 9
	}
	b.level = level
}

// CompressData compresses CompressibleData into a CompressedBlock
func (b *BaseCompressor) CompressData(data *CompressibleData) (*CompressedBlock, error) {
	// Stage 1: Apply delta encoding to timestamps if present
	if len(data.Timestamps) > 0 {
		data.Timestamps = b.deltaEncodeTimestamps(data.Timestamps)
	}

	// Stage 2: Apply dictionary substitution
	substitutedData := b.applyDictionarySubstitution(data)

	// Stage 3: Serialize to JSON
	jsonData, err := json.Marshal(substitutedData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data: %w", err)
	}
	originalSize := len(jsonData)

	// Stage 4: Compress with gzip (Go's stdlib - Brotli requires CGO)
	compressed, err := b.compress(jsonData)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	compressionRatio := float64(originalSize) / float64(len(compressed))

	return &CompressedBlock{
		OriginalSize:     originalSize,
		CompressedSize:   len(compressed),
		CompressionRatio: compressionRatio,
		Data:             compressed,
		Metadata: BlockMetadata{
			Format:     data.Format,
			Technique:  "v2-columnar-gzip",
			Dictionary: data.Dictionary,
			Patterns:   data.Patterns,
			Schema:     b.schemaToMap(data.Schema),
		},
	}, nil
}

// DecompressData decompresses a CompressedBlock back to CompressibleData
func (b *BaseCompressor) DecompressData(block *CompressedBlock) (*CompressibleData, error) {
	// Stage 1: Decompress
	decompressed, err := b.decompress(block.Data)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}

	// Stage 2: Deserialize from JSON
	var data CompressibleData
	if err := json.Unmarshal(decompressed, &data); err != nil {
		return nil, fmt.Errorf("failed to deserialize data: %w", err)
	}

	// Stage 3: Reverse dictionary substitution
	b.reverseDictionarySubstitution(&data)

	// Stage 4: Reverse delta encoding on timestamps
	if len(data.Timestamps) > 0 {
		data.Timestamps = b.deltaDecodeTimestamps(data.Timestamps)
	}

	return &data, nil
}

// deltaEncodeTimestamps applies delta encoding to timestamps
func (b *BaseCompressor) deltaEncodeTimestamps(timestamps []int64) []int64 {
	if len(timestamps) == 0 {
		return timestamps
	}

	result := make([]int64, len(timestamps))
	result[0] = timestamps[0] // First value is base

	for i := 1; i < len(timestamps); i++ {
		result[i] = timestamps[i] - timestamps[i-1] // Store delta
	}

	return result
}

// deltaDecodeTimestamps reverses delta encoding
func (b *BaseCompressor) deltaDecodeTimestamps(deltas []int64) []int64 {
	if len(deltas) == 0 {
		return deltas
	}

	result := make([]int64, len(deltas))
	result[0] = deltas[0] // First value is base

	for i := 1; i < len(deltas); i++ {
		result[i] = result[i-1] + deltas[i] // Accumulate deltas
	}

	return result
}

// applyDictionarySubstitution replaces common strings with dictionary indices
func (b *BaseCompressor) applyDictionarySubstitution(data *CompressibleData) *CompressibleData {
	// Already has dictionary from adapter, just return
	return data
}

// reverseDictionarySubstitution reverses dictionary substitution
func (b *BaseCompressor) reverseDictionarySubstitution(data *CompressibleData) {
	// Build reverse dictionary
	reverseDict := make(map[int]string)
	for k, v := range data.Dictionary {
		reverseDict[v] = k
	}

	// Apply reverse substitution to fields
	// (This is handled by each adapter's Reconstruct method)
}

// compress compresses data using gzip
func (b *BaseCompressor) compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, b.level)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompress decompresses gzip data
func (b *BaseCompressor) decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// schemaToMap converts Schema to map for JSON serialization
func (b *BaseCompressor) schemaToMap(schema Schema) map[string]interface{} {
	result := make(map[string]interface{})
	result["type"] = string(schema.Type)
	result["fields"] = schema.Fields

	types := make(map[string]string)
	for k, v := range schema.Types {
		types[k] = string(v)
	}
	result["types"] = types

	if len(schema.Extra) > 0 {
		for k, v := range schema.Extra {
			result[k] = v
		}
	}

	return result
}

// BuildDictionary builds a dictionary from string frequency
func BuildDictionary(items []string, minFrequency int) map[string]int {
	// Count frequency
	frequency := make(map[string]int)
	for _, item := range items {
		frequency[item]++
	}

	// Filter by minimum frequency and sort by frequency descending
	type kv struct {
		Key   string
		Value int
	}

	var sorted []kv
	for k, v := range frequency {
		if v >= minFrequency {
			sorted = append(sorted, kv{k, v})
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Value > sorted[j].Value
	})

	// Build dictionary with sequential indices
	dict := make(map[string]int)
	for i, kv := range sorted {
		if i >= MaxDictionarySize {
			break
		}
		dict[kv.Key] = i
	}

	return dict
}

// ClearCaches clears all compression caches
func (b *BaseCompressor) ClearCaches() {
	b.dictionaryCache.Clear()
	b.patternCache.Clear()
}
