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
 * Compression Types and Interfaces
 *
 * Defines the core interfaces for the V2 compression engine:
 * - CompressedBlock: Output of compression
 * - CompressibleData: Intermediate columnar format
 * - Adapter: Interface for format-specific compressors
 */

package compression

// CompressedBlock represents the output of compression
type CompressedBlock struct {
	OriginalSize     int
	CompressedSize   int
	CompressionRatio float64
	Data             []byte
	Metadata         BlockMetadata
}

// BlockMetadata contains information about the compressed data
type BlockMetadata struct {
	Format     string
	Technique  string
	Dictionary map[string]int
	Patterns   map[string]int
	Schema     map[string]interface{}
}

// FieldType represents the type of a field in the schema
type FieldType string

const (
	FieldTypeString  FieldType = "string"
	FieldTypeNumber  FieldType = "number"
	FieldTypeBoolean FieldType = "boolean"
	FieldTypeArray   FieldType = "array"
	FieldTypeObject  FieldType = "object"
)

// SchemaType represents the type of data layout
type SchemaType string

const (
	SchemaTypeColumnar SchemaType = "columnar"
	SchemaTypeNested   SchemaType = "nested"
	SchemaTypeFlat     SchemaType = "flat"
)

// Schema describes the structure of compressible data
type Schema struct {
	Type   SchemaType             `json:"type"`
	Fields []string               `json:"fields"`
	Types  map[string]FieldType   `json:"types"`
	Extra  map[string]interface{} `json:"extra,omitempty"` // Additional metadata for reconstruction
}

// CompressibleData is the intermediate format for compression
type CompressibleData struct {
	Format     string
	Schema     Schema
	Fields     map[string][]interface{}
	Dictionary map[string]int
	Patterns   map[string]int
	Timestamps []int64
}

// Adapter is the interface for format-specific compression adapters
type Adapter interface {
	// Name returns the adapter name for logging/debugging
	Name() string

	// CanHandle checks if this adapter can handle the given data
	CanHandle(data []byte, format string) bool

	// Parse converts raw bytes into structured format
	Parse(data []byte) (interface{}, error)

	// Extract transforms parsed data into compressible format
	Extract(parsed interface{}) (*CompressibleData, error)

	// Reconstruct converts compressible data back to original format
	Reconstruct(data *CompressibleData) (interface{}, error)

	// Serialize converts reconstructed data back to bytes
	Serialize(reconstructed interface{}, format string) ([]byte, error)
}

// AdapterRegistry manages all available compression adapters
type AdapterRegistry struct {
	adapters []Adapter
}

// NewAdapterRegistry creates a new adapter registry
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		adapters: make([]Adapter, 0),
	}
}

// Register adds an adapter to the registry
func (r *AdapterRegistry) Register(adapter Adapter) {
	r.adapters = append(r.adapters, adapter)
}

// SelectAdapter finds the best adapter for the given data
func (r *AdapterRegistry) SelectAdapter(data []byte, format string) Adapter {
	// Try exact format match first
	if format != "" {
		for _, adapter := range r.adapters {
			if adapter.CanHandle(data, format) {
				return adapter
			}
		}
	}

	// Try each adapter's canHandle without format hint
	for _, adapter := range r.adapters {
		if adapter.CanHandle(data, "") {
			return adapter
		}
	}

	return nil
}

// GetAdapters returns all registered adapters
func (r *AdapterRegistry) GetAdapters() []Adapter {
	result := make([]Adapter, len(r.adapters))
	copy(result, r.adapters)
	return result
}
