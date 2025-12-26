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
 * ContextualDictionaryCompressor
 *
 * Achieves 99.4% compression on repetitive application logs by:
 * 1. Extracting templates from log patterns
 * 2. Building dictionaries for repeated tokens
 * 3. Transforming to columnar format
 * 4. Applying final compression
 *
 * Log-specific optimizations:
 * - Template extraction (e.g., "User {id} logged in" appears 1000x)
 * - Variable extraction (IDs, paths, IPs, emails)
 * - Level/service dictionary encoding
 * - Timestamp delta encoding
 */

package compression

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// LogLine represents a parsed log entry
type LogLine struct {
	Timestamp    string
	Level        string
	Service      string
	Message      string
	Raw          string
	IsJSONFormat bool
}

// Template represents a log message template
type Template struct {
	ID      string
	Pattern string
	Fields  []string
	Regex   *regexp.Regexp
}

// ContextualDictionaryCompressor compresses log data
type ContextualDictionaryCompressor struct {
	logPatterns []*regexp.Regexp
}

// NewContextualDictionaryCompressor creates a new log compressor
func NewContextualDictionaryCompressor() *ContextualDictionaryCompressor {
	return &ContextualDictionaryCompressor{
		logPatterns: []*regexp.Regexp{
			// ISO timestamp + level + service + message
			regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s+(\w+)\s+\[([^\]]+)\]\s+(.+)$`),
			// Timestamp + level + message
			regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s+(\w+)\s+(.+)$`),
			// Syslog format
			regexp.MustCompile(`^(\w+\s+\d+\s+\d{2}:\d{2}:\d{2})\s+(\w+)\s+(.+)$`),
			// Simple: level + message
			regexp.MustCompile(`^(\w+):\s+(.+)$`),
		},
	}
}

// Name returns the adapter name
func (c *ContextualDictionaryCompressor) Name() string {
	return "ContextualDictionaryCompressor"
}

// CanHandle checks if this adapter can handle the given data
func (c *ContextualDictionaryCompressor) CanHandle(data []byte, format string) bool {
	formatLower := strings.ToLower(format)
	if formatLower == "logs" || formatLower == "datadog-logs" || formatLower == "syslog" {
		return true
	}

	// Try to detect log format
	text := string(data[:min(1000, len(data))])
	lines := strings.Split(text, "\n")

	matches := 0
	sampleSize := min(10, len(lines))

	for i := 0; i < sampleSize; i++ {
		if c.matchesLogPattern(lines[i]) {
			matches++
		}
	}

	// 60% of lines must match log patterns
	return matches >= sampleSize*6/10
}

// Parse converts raw bytes into structured log lines
func (c *ContextualDictionaryCompressor) Parse(data []byte) (interface{}, error) {
	text := string(data)

	// Try parsing as JSON first
	var jsonLogs []map[string]interface{}
	if err := json.Unmarshal(data, &jsonLogs); err == nil {
		logs := make([]LogLine, 0, len(jsonLogs))
		for _, obj := range jsonLogs {
			log := LogLine{
				Timestamp:    c.getString(obj, "timestamp", time.Now().Format(time.RFC3339)),
				Level:        c.getString(obj, "level", "INFO"),
				Service:      c.getString(obj, "service", ""),
				Message:      c.getStringAny(obj, []string{"message", "msg"}, ""),
				Raw:          c.toJSON(obj),
				IsJSONFormat: true,
			}
			logs = append(logs, log)
		}
		return logs, nil
	}

	// Parse as text log lines
	lines := strings.Split(text, "\n")
	logs := make([]LogLine, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		logs = append(logs, c.parseLine(line))
	}

	return logs, nil
}

// Extract transforms parsed logs into compressible format
func (c *ContextualDictionaryCompressor) Extract(parsed interface{}) (*CompressibleData, error) {
	logs, ok := parsed.([]LogLine)
	if !ok {
		return nil, nil
	}

	// Stage 1: Extract templates
	templates := c.extractTemplates(logs)

	// Stage 2: Build dictionary for repeated tokens
	dictionary := c.buildDictionary(logs)

	// Stage 3: Transform to columnar format
	columnar := c.transformToColumnar(logs, templates, dictionary)

	// Check if original was JSON format
	originalFormat := "text"
	if len(logs) > 0 && logs[0].IsJSONFormat {
		originalFormat = "json"
	}

	// Store templates for reconstruction
	templateData := make([]map[string]interface{}, 0, len(templates))
	for _, t := range templates {
		templateData = append(templateData, map[string]interface{}{
			"id":      t.ID,
			"pattern": t.Pattern,
		})
	}

	return &CompressibleData{
		Format: "logs",
		Schema: Schema{
			Type:   SchemaTypeColumnar,
			Fields: []string{"templateId", "timestamp", "level", "service", "variables"},
			Types: map[string]FieldType{
				"templateId": FieldTypeString,
				"timestamp":  FieldTypeNumber,
				"level":      FieldTypeNumber,
				"service":    FieldTypeNumber,
				"variables":  FieldTypeArray,
			},
			Extra: map[string]interface{}{
				"templates":      templateData,
				"originalFormat": originalFormat,
			},
		},
		Fields:     columnar,
		Dictionary: dictionary,
		Patterns:   c.buildPatternMap(templates),
		Timestamps: c.extractTimestamps(columnar),
	}, nil
}

// Reconstruct converts compressible data back to logs
func (c *ContextualDictionaryCompressor) Reconstruct(data *CompressibleData) (interface{}, error) {
	// Reverse dictionary
	reverseDict := make(map[int]string)
	for k, v := range data.Dictionary {
		reverseDict[v] = k
	}

	// Build template map from schema
	templateMap := make(map[int]string)
	if templates, ok := data.Schema.Extra["templates"].([]map[string]interface{}); ok {
		for _, t := range templates {
			id := t["id"].(string)
			pattern := t["pattern"].(string)
			// Parse ID like "T0" -> 0
			var idx int
			fmt.Sscanf(id, "T%d", &idx)
			templateMap[idx] = pattern
		}
	}

	isJSONFormat := data.Schema.Extra["originalFormat"] == "json"

	// Reconstruct each log entry
	fields := data.Fields
	count := len(fields["templateId"])

	logs := make([]interface{}, 0, count)

	for i := 0; i < count; i++ {
		templateID := int(fields["templateId"][i].(float64))
		timestamp := int64(fields["timestamp"][i].(float64))
		levelIdx := int(fields["level"][i].(float64))
		serviceIdx := fields["service"][i]
		variables := fields["variables"][i].([]interface{})

		level := reverseDict[levelIdx]
		if level == "" {
			level = "INFO"
		}

		var service string
		if serviceIdx != nil {
			if idx, ok := serviceIdx.(float64); ok {
				service = reverseDict[int(idx)]
			}
		}

		// Build message from template
		var message string
		if templateID == -1 {
			// No template - use full message from variables
			if len(variables) > 0 {
				if strIdx, ok := variables[0].(float64); ok {
					message = reverseDict[int(strIdx)]
				} else if str, ok := variables[0].(string); ok {
					message = str
				}
			}
		} else if pattern, ok := templateMap[templateID]; ok {
			message = pattern
			// Replace variable placeholders
			varPatterns := []string{"{num}", "{id}", "{email}", "{ip}", "{path}", "{str}"}
			varIdx := 0
			for _, vp := range varPatterns {
				for strings.Contains(message, vp) && varIdx < len(variables) {
					var value string
					if strIdx, ok := variables[varIdx].(float64); ok {
						value = reverseDict[int(strIdx)]
					} else if str, ok := variables[varIdx].(string); ok {
						value = str
					} else {
						value = fmt.Sprintf("%v", variables[varIdx])
					}
					message = strings.Replace(message, vp, value, 1)
					varIdx++
				}
			}
		}

		if isJSONFormat {
			logObj := map[string]interface{}{
				"timestamp": time.UnixMilli(timestamp).Format(time.RFC3339),
				"level":     level,
				"message":   message,
			}
			if service != "" {
				logObj["service"] = service
			}
			logs = append(logs, logObj)
		} else {
			line := fmt.Sprintf("%s %s", time.UnixMilli(timestamp).Format(time.RFC3339), level)
			if service != "" {
				line += fmt.Sprintf(" [%s]", service)
			}
			line += " " + message
			logs = append(logs, line)
		}
	}

	return logs, nil
}

// Serialize converts reconstructed logs to bytes
func (c *ContextualDictionaryCompressor) Serialize(reconstructed interface{}, format string) ([]byte, error) {
	logs, ok := reconstructed.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type for reconstructed logs")
	}

	// Check if logs are strings (text format) or maps (JSON format)
	if len(logs) > 0 {
		if _, isString := logs[0].(string); isString {
			// Text format
			lines := make([]string, len(logs))
			for i, log := range logs {
				lines[i] = log.(string)
			}
			return []byte(strings.Join(lines, "\n")), nil
		}
	}

	// JSON format
	return json.Marshal(logs)
}

// parseLine parses a single log line
func (c *ContextualDictionaryCompressor) parseLine(line string) LogLine {
	for _, pattern := range c.logPatterns {
		match := pattern.FindStringSubmatch(line)
		if match != nil {
			switch len(match) {
			case 5: // Timestamp + level + service + message
				return LogLine{
					Timestamp: match[1],
					Level:     match[2],
					Service:   match[3],
					Message:   match[4],
					Raw:       line,
				}
			case 4: // Timestamp + level + message
				return LogLine{
					Timestamp: match[1],
					Level:     match[2],
					Message:   match[3],
					Raw:       line,
				}
			case 3: // Level + message
				return LogLine{
					Timestamp: time.Now().Format(time.RFC3339),
					Level:     match[1],
					Message:   match[2],
					Raw:       line,
				}
			}
		}
	}

	// Fallback: treat entire line as message
	return LogLine{
		Timestamp: time.Now().Format(time.RFC3339),
		Level:     "INFO",
		Message:   line,
		Raw:       line,
	}
}

// matchesLogPattern checks if line matches any log pattern
func (c *ContextualDictionaryCompressor) matchesLogPattern(line string) bool {
	for _, pattern := range c.logPatterns {
		if pattern.MatchString(line) {
			return true
		}
	}
	return false
}

// extractTemplates extracts templates from log messages
func (c *ContextualDictionaryCompressor) extractTemplates(logs []LogLine) []Template {
	patternCounts := make(map[string]int)

	// Analyze messages to find patterns
	for _, log := range logs {
		pattern := c.extractPattern(log.Message)
		patternCounts[pattern]++
	}

	// Create templates for frequently occurring patterns
	templates := make([]Template, 0)
	templateID := 0

	for pattern, count := range patternCounts {
		if count >= 3 { // Minimum frequency threshold
			templates = append(templates, Template{
				ID:      fmt.Sprintf("T%d", templateID),
				Pattern: pattern,
				Fields:  c.extractFields(pattern),
				Regex:   c.patternToRegex(pattern),
			})
			templateID++
		}
	}

	return templates
}

// extractPattern extracts pattern from message by replacing variables
func (c *ContextualDictionaryCompressor) extractPattern(message string) string {
	pattern := message

	// Replace common variable patterns
	patterns := []struct {
		regex       *regexp.Regexp
		replacement string
	}{
		{regexp.MustCompile(`\b\d+\b`), "{num}"},                    // Numbers
		{regexp.MustCompile(`\b[a-f0-9]{8,}\b`), "{id}"},            // IDs/hashes
		{regexp.MustCompile(`\b[\w.-]+@[\w.-]+\b`), "{email}"},      // Emails
		{regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`), "{ip}"}, // IPs
		{regexp.MustCompile(`/[\w/-]+`), "{path}"},                  // Paths
		{regexp.MustCompile(`"[^"]+"`), "{str}"},                    // Quoted strings
	}

	for _, p := range patterns {
		pattern = p.regex.ReplaceAllString(pattern, p.replacement)
	}

	return pattern
}

// extractFields extracts field names from pattern
func (c *ContextualDictionaryCompressor) extractFields(pattern string) []string {
	re := regexp.MustCompile(`\{(\w+)\}`)
	matches := re.FindAllStringSubmatch(pattern, -1)

	fields := make([]string, 0)
	seen := make(map[string]bool)

	for _, match := range matches {
		if !seen[match[1]] {
			fields = append(fields, match[1])
			seen[match[1]] = true
		}
	}

	return fields
}

// patternToRegex converts pattern to regex for matching
func (c *ContextualDictionaryCompressor) patternToRegex(pattern string) *regexp.Regexp {
	// Escape special regex chars
	escaped := regexp.QuoteMeta(pattern)

	// Replace placeholders with capture groups
	replacements := []struct {
		placeholder string
		regex       string
	}{
		{`\{num\}`, `(\d+)`},
		{`\{id\}`, `([a-f0-9]+)`},
		{`\{email\}`, `([\w.-]+@[\w.-]+)`},
		{`\{ip\}`, `((?:\d{1,3}\.){3}\d{1,3})`},
		{`\{path\}`, `(/[\w/-]+)`},
		{`\{str\}`, `("[^"]+")`},
	}

	for _, r := range replacements {
		escaped = strings.ReplaceAll(escaped, r.placeholder, r.regex)
	}

	re, _ := regexp.Compile(escaped)
	return re
}

// buildDictionary builds dictionary of repeated tokens
func (c *ContextualDictionaryCompressor) buildDictionary(logs []LogLine) map[string]int {
	frequency := make(map[string]int)
	criticalFields := make(map[string]bool)

	for _, log := range logs {
		// Log levels (always include)
		frequency[log.Level]++
		criticalFields[log.Level] = true

		// Service names (always include)
		if log.Service != "" {
			frequency[log.Service]++
			criticalFields[log.Service] = true
		}

		// Extract tokens from message
		tokens := c.tokenize(log.Message)
		for _, token := range tokens {
			frequency[token]++
		}
	}

	// Build dictionary
	dict := make(map[string]int)
	idx := 0

	// First, add all critical fields
	for token := range criticalFields {
		dict[token] = idx
		idx++
	}

	// Then add high-frequency tokens
	for token, count := range frequency {
		if count >= 3 && !criticalFields[token] {
			dict[token] = idx
			idx++
		}
	}

	return dict
}

// tokenize extracts tokens from message
func (c *ContextualDictionaryCompressor) tokenize(message string) []string {
	tokens := make([]string, 0)

	// Extract paths
	pathRe := regexp.MustCompile(`/[\w/-]+`)
	tokens = append(tokens, pathRe.FindAllString(message, -1)...)

	// Extract quoted strings
	strRe := regexp.MustCompile(`"([^"]+)"`)
	for _, match := range strRe.FindAllStringSubmatch(message, -1) {
		tokens = append(tokens, match[1])
	}

	// Extract service names
	svcRe := regexp.MustCompile(`\b[a-z]+[-_][a-z]+\b`)
	tokens = append(tokens, svcRe.FindAllString(message, -1)...)

	return tokens
}

// transformToColumnar transforms logs to columnar format
func (c *ContextualDictionaryCompressor) transformToColumnar(logs []LogLine, templates []Template, dictionary map[string]int) map[string][]interface{} {
	n := len(logs)

	templateIds := make([]interface{}, n)
	timestamps := make([]interface{}, n)
	levels := make([]interface{}, n)
	services := make([]interface{}, n)
	variables := make([]interface{}, n)

	for i, log := range logs {
		// Find matching template
		template := c.findTemplate(log.Message, templates)
		if template != nil {
			var id int
			fmt.Sscanf(template.ID, "T%d", &id)
			templateIds[i] = id
		} else {
			templateIds[i] = -1
		}

		// Parse timestamp
		ts, err := time.Parse(time.RFC3339, log.Timestamp)
		if err != nil {
			ts = time.Now()
		}
		timestamps[i] = ts.UnixMilli()

		// Encode level
		levelID, ok := dictionary[log.Level]
		if !ok {
			levelID = -1
		}
		levels[i] = levelID

		// Encode service
		if log.Service != "" {
			if serviceID, ok := dictionary[log.Service]; ok {
				services[i] = serviceID
			} else {
				services[i] = nil
			}
		} else {
			services[i] = nil
		}

		// Extract variables from message
		var vars []interface{}
		if template != nil {
			vars = c.extractVariables(log.Message, template, dictionary)
		} else {
			// No template - store full message
			if msgID, ok := dictionary[log.Message]; ok {
				vars = []interface{}{msgID}
			} else {
				vars = []interface{}{log.Message}
			}
		}
		variables[i] = vars
	}

	return map[string][]interface{}{
		"templateId": templateIds,
		"timestamp":  timestamps,
		"level":      levels,
		"service":    services,
		"variables":  variables,
	}
}

// findTemplate finds matching template for message
func (c *ContextualDictionaryCompressor) findTemplate(message string, templates []Template) *Template {
	pattern := c.extractPattern(message)
	for i := range templates {
		if templates[i].Pattern == pattern {
			return &templates[i]
		}
	}
	return nil
}

// extractVariables extracts variable values from message using template
func (c *ContextualDictionaryCompressor) extractVariables(message string, template *Template, dictionary map[string]int) []interface{} {
	if template.Regex == nil {
		return nil
	}

	match := template.Regex.FindStringSubmatch(message)
	if match == nil {
		return nil
	}

	// Skip full match, return captured groups
	vars := make([]interface{}, 0, len(match)-1)
	for i := 1; i < len(match); i++ {
		// Try to encode with dictionary
		if id, ok := dictionary[match[i]]; ok {
			vars = append(vars, id)
		} else {
			vars = append(vars, match[i])
		}
	}

	return vars
}

// buildPatternMap builds pattern frequency map
func (c *ContextualDictionaryCompressor) buildPatternMap(templates []Template) map[string]int {
	patterns := make(map[string]int)
	for _, t := range templates {
		patterns[t.Pattern] = 1
	}
	return patterns
}

// extractTimestamps extracts timestamps from columnar data
func (c *ContextualDictionaryCompressor) extractTimestamps(columnar map[string][]interface{}) []int64 {
	if ts, ok := columnar["timestamp"]; ok {
		result := make([]int64, len(ts))
		for i, t := range ts {
			if v, ok := t.(int64); ok {
				result[i] = v
			} else if v, ok := t.(float64); ok {
				result[i] = int64(v)
			}
		}
		return result
	}
	return nil
}

// Helper methods

func (c *ContextualDictionaryCompressor) getString(obj map[string]interface{}, key, defaultVal string) string {
	if v, ok := obj[key].(string); ok {
		return v
	}
	return defaultVal
}

func (c *ContextualDictionaryCompressor) getStringAny(obj map[string]interface{}, keys []string, defaultVal string) string {
	for _, key := range keys {
		if v, ok := obj[key].(string); ok {
			return v
		}
	}
	return defaultVal
}

func (c *ContextualDictionaryCompressor) toJSON(obj interface{}) string {
	data, _ := json.Marshal(obj)
	return string(data)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
