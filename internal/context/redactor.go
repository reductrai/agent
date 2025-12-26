package context

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// PIIRedactor removes sensitive information from telemetry data
type PIIRedactor struct {
	patterns []*redactionPattern
}

type redactionPattern struct {
	regex       *regexp.Regexp
	replacement string
	name        string
}

// NewPIIRedactor creates a new redactor with default patterns
func NewPIIRedactor() *PIIRedactor {
	return &PIIRedactor{
		patterns: []*redactionPattern{
			// Email addresses
			{
				regex:       regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`),
				replacement: "[EMAIL]",
				name:        "email",
			},
			// IP addresses (v4)
			{
				regex:       regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`),
				replacement: "[IP]",
				name:        "ipv4",
			},
			// IP addresses (v6) - simplified pattern
			{
				regex:       regexp.MustCompile(`([0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}`),
				replacement: "[IPV6]",
				name:        "ipv6",
			},
			// JWT tokens (three base64 segments separated by dots)
			{
				regex:       regexp.MustCompile(`eyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*`),
				replacement: "[JWT]",
				name:        "jwt",
			},
			// API keys and secrets (common patterns)
			{
				regex:       regexp.MustCompile(`(?i)(api[_-]?key|apikey|api_secret|secret_key|access_token|auth_token|private_key)[=:\s]["']?([a-zA-Z0-9_\-]{16,})["']?`),
				replacement: "${1}=[REDACTED]",
				name:        "api_key",
			},
			// Bearer tokens
			{
				regex:       regexp.MustCompile(`(?i)bearer\s+[a-zA-Z0-9_\-\.]{20,}`),
				replacement: "Bearer [TOKEN]",
				name:        "bearer",
			},
			// AWS access keys
			{
				regex:       regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
				replacement: "[AWS_KEY]",
				name:        "aws_access_key",
			},
			// AWS secret keys (40 char base64)
			{
				regex:       regexp.MustCompile(`(?i)(aws_secret_access_key|secret_access_key)[=:\s]["']?([a-zA-Z0-9/+=]{40})["']?`),
				replacement: "${1}=[REDACTED]",
				name:        "aws_secret",
			},
			// Credit card numbers (basic pattern)
			{
				regex:       regexp.MustCompile(`\b(?:\d[ \-]*?){13,19}\b`),
				replacement: "[CARD]",
				name:        "credit_card",
			},
			// Phone numbers (various formats)
			{
				regex:       regexp.MustCompile(`\b\+?1?[\-.\s]?\(?\d{3}\)?[\-.\s]?\d{3}[\-.\s]?\d{4}\b`),
				replacement: "[PHONE]",
				name:        "phone",
			},
			// Social Security Numbers
			{
				regex:       regexp.MustCompile(`\b\d{3}[\-\s]?\d{2}[\-\s]?\d{4}\b`),
				replacement: "[SSN]",
				name:        "ssn",
			},
			// Passwords in connection strings
			{
				regex:       regexp.MustCompile(`://([^:]+):([^@]+)@`),
				replacement: "://[USER]:[PASS]@",
				name:        "connection_password",
			},
			// Generic password fields
			{
				regex:       regexp.MustCompile(`(?i)(password|passwd|pwd)[=:\s]["']?([^\s"']{4,})["']?`),
				replacement: "${1}=[REDACTED]",
				name:        "password",
			},
			// Private keys (PEM format)
			{
				regex:       regexp.MustCompile(`-----BEGIN [A-Z ]+ PRIVATE KEY-----[\s\S]*?-----END [A-Z ]+ PRIVATE KEY-----`),
				replacement: "[PRIVATE_KEY]",
				name:        "private_key",
			},
			// Slack tokens
			{
				regex:       regexp.MustCompile(`xox[baprs]-[0-9a-zA-Z\-]{10,}`),
				replacement: "[SLACK_TOKEN]",
				name:        "slack_token",
			},
			// GitHub tokens
			{
				regex:       regexp.MustCompile(`ghp_[a-zA-Z0-9]{36}`),
				replacement: "[GITHUB_TOKEN]",
				name:        "github_token",
			},
			// Stripe keys
			{
				regex:       regexp.MustCompile(`sk_(live|test)_[a-zA-Z0-9]{24,}`),
				replacement: "[STRIPE_KEY]",
				name:        "stripe_key",
			},
		},
	}
}

// Redact removes PII from a string
func (r *PIIRedactor) Redact(input string) string {
	result := input
	for _, p := range r.patterns {
		result = p.regex.ReplaceAllString(result, p.replacement)
	}
	return result
}

// RedactUUID partially redacts UUIDs while keeping first 8 chars for correlation
func (r *PIIRedactor) RedactUUID(input string) string {
	uuidPattern := regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	return uuidPattern.ReplaceAllStringFunc(input, func(match string) string {
		if len(match) >= 8 {
			return match[:8] + "-****-****-****-************"
		}
		return "[UUID]"
	})
}

// ExtractTemplate converts a log message to a template and returns a hash for deduplication
// Example: "User john@example.com failed login from 192.168.1.1"
// Returns: "User [EMAIL] failed login from [IP]", "a1b2c3d4"
func (r *PIIRedactor) ExtractTemplate(message string) (template string, hash string) {
	// First apply standard redactions
	template = r.Redact(message)

	// Also redact UUIDs
	template = r.RedactUUID(template)

	// Redact numeric IDs (sequences of 5+ digits that aren't part of timestamps)
	numericIDPattern := regexp.MustCompile(`\b\d{5,}\b`)
	template = numericIDPattern.ReplaceAllString(template, "[ID]")

	// Generate short hash for deduplication
	h := sha256.Sum256([]byte(template))
	hash = hex.EncodeToString(h[:8])

	return template, hash
}

// RedactPath removes sensitive path parameters from URL paths
// Example: "/users/123/orders/456" -> "/users/[ID]/orders/[ID]"
// Example: "/users/a1b2c3d4-..." -> "/users/[UUID]"
func (r *PIIRedactor) RedactPath(path string) string {
	// Replace numeric IDs in paths
	numericPattern := regexp.MustCompile(`/\d+(/|$)`)
	result := numericPattern.ReplaceAllString(path, "/[ID]$1")

	// Replace UUIDs in paths
	uuidPattern := regexp.MustCompile(`/[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}(/|$)`)
	result = uuidPattern.ReplaceAllString(result, "/[UUID]$2")

	// Replace common ID formats (alphanumeric 8+ chars that look like IDs)
	alphaIDPattern := regexp.MustCompile(`/[a-zA-Z0-9_-]{20,}(/|$)`)
	result = alphaIDPattern.ReplaceAllString(result, "/[TOKEN]$1")

	return result
}

// RedactJSON redacts values in JSON-like strings (for tags/attributes)
func (r *PIIRedactor) RedactJSON(input string) string {
	// Redact common sensitive JSON keys
	sensitiveKeys := []string{
		"user_id", "userId", "customer_id", "customerId",
		"email", "phone", "address", "ssn",
		"token", "api_key", "apiKey", "secret",
		"password", "auth", "authorization",
	}

	result := input
	for _, key := range sensitiveKeys {
		// Match "key": "value" or "key":"value"
		pattern := regexp.MustCompile(`"` + key + `"\s*:\s*"[^"]*"`)
		result = pattern.ReplaceAllString(result, `"`+key+`":"[REDACTED]"`)

		// Match key=value
		pattern2 := regexp.MustCompile(`(?i)` + key + `=([^\s&]+)`)
		result = pattern2.ReplaceAllString(result, key+"=[REDACTED]")
	}

	// Apply standard redactions to catch any remaining PII
	result = r.Redact(result)

	return result
}

// ExtractErrorType tries to identify the type of error from a message
func ExtractErrorType(message string) string {
	messageLower := strings.ToLower(message)

	// Timeout errors
	if strings.Contains(messageLower, "timeout") ||
		strings.Contains(messageLower, "timed out") ||
		strings.Contains(messageLower, "deadline exceeded") {
		return "timeout"
	}

	// Connection errors
	if strings.Contains(messageLower, "connection refused") ||
		strings.Contains(messageLower, "connection reset") ||
		strings.Contains(messageLower, "no route to host") ||
		strings.Contains(messageLower, "network unreachable") {
		return "connection_error"
	}

	// DNS errors
	if strings.Contains(messageLower, "dns") ||
		strings.Contains(messageLower, "no such host") ||
		strings.Contains(messageLower, "name resolution") {
		return "dns_error"
	}

	// TLS/SSL errors
	if strings.Contains(messageLower, "tls") ||
		strings.Contains(messageLower, "ssl") ||
		strings.Contains(messageLower, "certificate") {
		return "tls_error"
	}

	// Out of memory
	if strings.Contains(messageLower, "out of memory") ||
		strings.Contains(messageLower, "oom") ||
		strings.Contains(messageLower, "memory limit") {
		return "memory_error"
	}

	// Database errors
	if strings.Contains(messageLower, "database") ||
		strings.Contains(messageLower, "sql") ||
		strings.Contains(messageLower, "query failed") ||
		strings.Contains(messageLower, "connection pool") {
		return "database_error"
	}

	// Authentication/Authorization
	if strings.Contains(messageLower, "unauthorized") ||
		strings.Contains(messageLower, "forbidden") ||
		strings.Contains(messageLower, "authentication") ||
		strings.Contains(messageLower, "permission denied") {
		return "auth_error"
	}

	// Rate limiting
	if strings.Contains(messageLower, "rate limit") ||
		strings.Contains(messageLower, "too many requests") ||
		strings.Contains(messageLower, "throttl") {
		return "rate_limit"
	}

	// Generic server errors
	if strings.Contains(messageLower, "internal server error") ||
		strings.Contains(messageLower, "500") {
		return "server_error"
	}

	// Bad request
	if strings.Contains(messageLower, "bad request") ||
		strings.Contains(messageLower, "invalid") ||
		strings.Contains(messageLower, "malformed") {
		return "validation_error"
	}

	return "unknown"
}
