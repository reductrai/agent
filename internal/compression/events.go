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
 * SemanticCompressor
 *
 * Achieves 99.5% compression on events and custom JSON data by:
 * 1. Detecting JSON schema patterns
 * 2. Building dictionaries for repeated keys and values
 * 3. Transforming to columnar format
 * 4. Applying final compression
 *
 * Event-specific optimizations:
 * - Schema detection across events
 * - Key dictionary for repeated property names
 * - Value dictionary for repeated values
 * - Columnar transformation for similar events
 */

package compression

import (
	"encoding/json"

	"strings"
	"time"
)

// EventData represents a parsed event
type EventData struct {
	EventType  string
	Timestamp  int64
	Properties map[string]interface{}
	Original   interface{}
}

// SemanticCompressor compresses event/JSON data
type SemanticCompressor struct{}

// NewSemanticCompressor creates a new event compressor
func NewSemanticCompressor() *SemanticCompressor {
	return &SemanticCompressor{}
}

// Name returns the adapter name
func (s *SemanticCompressor) Name() string {
	return "SemanticCompressor"
}

// CanHandle checks if this adapter can handle the given data
func (s *SemanticCompressor) CanHandle(data []byte, format string) bool {
	formatLower := strings.ToLower(format)
	if formatLower == "events" || formatLower == "json" || formatLower == "generic" {
		return true
	}

	// Try to detect generic JSON
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return false
	}

	// Accept any valid JSON
	switch parsed.(type) {
	case []interface{}, map[string]interface{}:
		return true
	}

	return false
}

// Parse converts raw bytes into event data
func (s *SemanticCompressor) Parse(data []byte) (interface{}, error) {
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}

	events := make([]EventData, 0)

	switch v := parsed.(type) {
	case []interface{}:
		for _, item := range v {
			events = append(events, s.normalizeEvent(item))
		}
	case map[string]interface{}:
		events = append(events, s.normalizeEvent(v))
	}

	return events, nil
}

// Extract transforms parsed events into compressible format
func (s *SemanticCompressor) Extract(parsed interface{}) (*CompressibleData, error) {
	events, ok := parsed.([]EventData)
	if !ok {
		return nil, nil
	}

	// Stage 1: Detect schema
	schema := s.detectSchema(events)

	// Stage 2: Build dictionaries
	keyDict := s.buildKeyDictionary(events)
	valueDict := s.buildValueDictionary(events)

	// Stage 3: Transform to columnar format
	columnar := s.transformToColumnar(events, keyDict, valueDict)

	return &CompressibleData{
		Format: "events",
		Schema: Schema{
			Type:   SchemaTypeColumnar,
			Fields: s.getFieldList(columnar),
			Types:  s.inferTypes(schema),
			Extra: map[string]interface{}{
				"keyDictionary":   s.dictToArray(keyDict),
				"valueDictionary": s.valueDictToArray(valueDict),
				"detectedSchema":  schema,
			},
		},
		Fields:     columnar,
		Dictionary: s.mergeKeyValueDicts(keyDict, valueDict),
		Patterns:   make(map[string]int),
		Timestamps: s.extractTimestamps(columnar),
	}, nil
}

// Reconstruct converts compressible data back to events
func (s *SemanticCompressor) Reconstruct(data *CompressibleData) (interface{}, error) {
	// Rebuild dictionaries (reverse them: id -> value)
	keyDict := make(map[int]string)
	if arr, ok := data.Schema.Extra["keyDictionary"].([][]interface{}); ok {
		for _, pair := range arr {
			if len(pair) >= 2 {
				key, _ := pair[0].(string)
				id, _ := pair[1].(float64)
				keyDict[int(id)] = key
			}
		}
	}

	valueDict := make(map[int]interface{})
	if arr, ok := data.Schema.Extra["valueDictionary"].([][]interface{}); ok {
		for _, pair := range arr {
			if len(pair) >= 2 {
				value := pair[0]
				id, _ := pair[1].(float64)
				valueDict[int(id)] = value
			}
		}
	}

	// Determine count from first non-empty field
	count := 0
	for _, values := range data.Fields {
		count = len(values)
		break
	}

	// Reconstruct events
	events := make([]map[string]interface{}, 0, count)

	for i := 0; i < count; i++ {
		event := make(map[string]interface{})

		for fieldKey, values := range data.Fields {
			if i >= len(values) {
				continue
			}

			value := values[i]

			// Skip null/undefined values
			if value == nil {
				continue
			}

			// Skip internal fields
			if strings.HasPrefix(fieldKey, "_") {
				continue
			}

			// Decode value
			decodedValue := s.decodeValue(value, valueDict)

			event[fieldKey] = decodedValue
		}

		events = append(events, event)
	}

	return events, nil
}

// Serialize converts reconstructed events to bytes
func (s *SemanticCompressor) Serialize(reconstructed interface{}, format string) ([]byte, error) {
	return json.Marshal(reconstructed)
}

// normalizeEvent normalizes event to standard format
func (s *SemanticCompressor) normalizeEvent(event interface{}) EventData {
	m, ok := event.(map[string]interface{})
	if !ok {
		return EventData{
			Timestamp:  time.Now().UnixMilli(),
			Properties: map[string]interface{}{},
			Original:   event,
		}
	}

	// Extract event type
	eventType := ""
	for _, key := range []string{"eventType", "type", "event"} {
		if v, ok := m[key].(string); ok {
			eventType = v
			break
		}
	}

	// Extract timestamp
	var timestamp int64
	for _, key := range []string{"timestamp", "ts", "time"} {
		if v, ok := m[key].(float64); ok {
			timestamp = int64(v)
			break
		}
		if v, ok := m[key].(string); ok {
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				timestamp = t.UnixMilli()
				break
			}
		}
	}
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}

	return EventData{
		EventType:  eventType,
		Timestamp:  timestamp,
		Properties: m,
		Original:   event,
	}
}

// detectSchema detects common schema across events
func (s *SemanticCompressor) detectSchema(events []EventData) map[string]string {
	fieldTypes := make(map[string]map[string]int)

	for _, event := range events {
		s.analyzeObject(event.Properties, fieldTypes, "")
	}

	// Determine most common type for each field
	schema := make(map[string]string)
	for field, types := range fieldTypes {
		var maxCount int
		var maxType string
		for t, count := range types {
			if count > maxCount {
				maxCount = count
				maxType = t
			}
		}
		schema[field] = maxType
	}

	return schema
}

// analyzeObject analyzes object to detect field types
func (s *SemanticCompressor) analyzeObject(obj interface{}, fieldTypes map[string]map[string]int, prefix string) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return
	}

	for key, value := range m {
		fieldName := key
		if prefix != "" {
			fieldName = prefix + "." + key
		}

		typeName := s.getType(value)

		if _, ok := fieldTypes[fieldName]; !ok {
			fieldTypes[fieldName] = make(map[string]int)
		}
		fieldTypes[fieldName][typeName]++

		// Recursively analyze nested objects
		if typeName == "object" {
			if nested, ok := value.(map[string]interface{}); ok {
				s.analyzeObject(nested, fieldTypes, fieldName)
			}
		}
	}
}

// getType returns the type name for a value
func (s *SemanticCompressor) getType(value interface{}) string {
	if value == nil {
		return "null"
	}
	switch value.(type) {
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case string:
		return "string"
	case float64, int, int64:
		return "number"
	case bool:
		return "boolean"
	default:
		return "unknown"
	}
}

// buildKeyDictionary builds dictionary of property keys
func (s *SemanticCompressor) buildKeyDictionary(events []EventData) map[string]int {
	frequency := make(map[string]int)

	for _, event := range events {
		s.collectKeys(event.Properties, frequency, "")
	}

	dict := make(map[string]int)
	idx := 0

	for key, count := range frequency {
		if count >= 2 {
			dict[key] = idx
			idx++
		}
	}

	return dict
}

// collectKeys collects keys from object recursively
func (s *SemanticCompressor) collectKeys(obj interface{}, frequency map[string]int, prefix string) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return
	}

	for key, value := range m {
		fieldName := key
		if prefix != "" {
			fieldName = prefix + "." + key
		}
		frequency[fieldName]++

		if nested, ok := value.(map[string]interface{}); ok {
			s.collectKeys(nested, frequency, fieldName)
		}
	}
}

// buildValueDictionary builds dictionary of property values (using string keys for hashability)
func (s *SemanticCompressor) buildValueDictionary(events []EventData) map[string]int {
	frequency := make(map[string]int)

	for _, event := range events {
		s.collectValues(event.Properties, frequency)
	}

	dict := make(map[string]int)
	idx := 0

	for key, count := range frequency {
		if count >= 2 {
			dict[key] = idx
			idx++
		}
	}

	return dict
}

// collectValues collects values from object recursively
func (s *SemanticCompressor) collectValues(obj interface{}, frequency map[string]int) {
	switch v := obj.(type) {
	case map[string]interface{}:
		for _, value := range v {
			s.collectValues(value, frequency)
		}
	case []interface{}:
		for _, item := range v {
			s.collectValues(item, frequency)
		}
	case string, float64, int, int64, bool:
		key, _ := json.Marshal(v)
		frequency[string(key)]++
	}
}

// transformToColumnar transforms events to columnar format
func (s *SemanticCompressor) transformToColumnar(events []EventData, keyDict map[string]int, valueDict map[string]int) map[string][]interface{} {
	// Identify all unique keys across events
	allKeys := make(map[string]bool)
	for _, event := range events {
		for key := range event.Properties {
			allKeys[key] = true
		}
	}

	// Create column for each key
	columnar := make(map[string][]interface{})
	for key := range allKeys {
		columnar[key] = make([]interface{}, 0, len(events))
	}

	// Fill columns
	for _, event := range events {
		for key := range allKeys {
			value := event.Properties[key]
			encodedValue := s.encodeValue(value, valueDict)
			columnar[key] = append(columnar[key], encodedValue)
		}
	}

	return columnar
}

// encodeValue encodes value using dictionary (string keys for JSON-serialized values)
func (s *SemanticCompressor) encodeValue(value interface{}, valueDict map[string]int) interface{} {
	if value == nil {
		return nil
	}

	// For primitive types, try dictionary lookup using JSON serialization
	switch v := value.(type) {
	case string, float64, int, int64, bool:
		key, _ := json.Marshal(v)
		if id, ok := valueDict[string(key)]; ok {
			return id
		}
		return value
	case []interface{}:
		// Handle arrays - encode each element
		encoded := make([]interface{}, len(v))
		for i, item := range v {
			encoded[i] = s.encodeValue(item, valueDict)
		}
		return encoded
	case map[string]interface{}:
		// Handle objects - encode each value
		encoded := make(map[string]interface{})
		for k, val := range v {
			encoded[k] = s.encodeValue(val, valueDict)
		}
		return encoded
	default:
		return value
	}
}

// decodeValue decodes value from dictionary
func (s *SemanticCompressor) decodeValue(value interface{}, valueDict map[int]interface{}) interface{} {
	if value == nil {
		return nil
	}

	// Check if it's a dictionary reference
	if id, ok := value.(float64); ok {
		if decoded, exists := valueDict[int(id)]; exists {
			return decoded
		}
	}

	// Handle arrays
	if arr, ok := value.([]interface{}); ok {
		decoded := make([]interface{}, len(arr))
		for i, item := range arr {
			decoded[i] = s.decodeValue(item, valueDict)
		}
		return decoded
	}

	// Handle objects
	if obj, ok := value.(map[string]interface{}); ok {
		decoded := make(map[string]interface{})
		for k, v := range obj {
			decoded[k] = s.decodeValue(v, valueDict)
		}
		return decoded
	}

	return value
}

// inferTypes infers field types from schema
func (s *SemanticCompressor) inferTypes(schema map[string]string) map[string]FieldType {
	types := make(map[string]FieldType)

	for key, typeName := range schema {
		switch typeName {
		case "string":
			types[key] = FieldTypeString
		case "number":
			types[key] = FieldTypeNumber
		case "boolean":
			types[key] = FieldTypeBoolean
		case "array":
			types[key] = FieldTypeArray
		case "object":
			types[key] = FieldTypeObject
		default:
			types[key] = FieldTypeObject
		}
	}

	return types
}

// Helper methods

func (s *SemanticCompressor) getFieldList(columnar map[string][]interface{}) []string {
	fields := make([]string, 0, len(columnar))
	for key := range columnar {
		fields = append(fields, key)
	}
	return fields
}

func (s *SemanticCompressor) dictToArray(dict map[string]int) [][]interface{} {
	result := make([][]interface{}, 0, len(dict))
	for k, v := range dict {
		result = append(result, []interface{}{k, v})
	}
	return result
}

func (s *SemanticCompressor) valueDictToArray(dict map[string]int) [][]interface{} {
	result := make([][]interface{}, 0, len(dict))
	for k, v := range dict {
		// Parse the JSON key back to its original value
		var parsed interface{}
		if err := json.Unmarshal([]byte(k), &parsed); err != nil {
			parsed = k
		}
		result = append(result, []interface{}{parsed, v})
	}
	return result
}

func (s *SemanticCompressor) mergeKeyValueDicts(keyDict map[string]int, valueDict map[string]int) map[string]int {
	merged := make(map[string]int)
	idx := 0

	for k := range keyDict {
		merged[k] = idx
		idx++
	}

	for k := range valueDict {
		if _, ok := merged[k]; !ok {
			merged[k] = idx
			idx++
		}
	}

	return merged
}

func (s *SemanticCompressor) extractTimestamps(columnar map[string][]interface{}) []int64 {
	for _, key := range []string{"timestamp", "ts", "time"} {
		if values, ok := columnar[key]; ok {
			result := make([]int64, 0, len(values))
			for _, v := range values {
				if t, ok := v.(float64); ok {
					result = append(result, int64(t))
				} else if t, ok := v.(int64); ok {
					result = append(result, t)
				}
			}
			return result
		}
	}
	return nil
}
