package storage

import (
	"fmt"
	"regexp"
	"strings"
)

// ColumnInfo represents metadata about a table column
type ColumnInfo struct {
	Name string
	Type string
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Name     string
	Columns  []ColumnInfo
	RowCount int64
}

// ShowTables returns all tables in the database (dynamic discovery)
func (d *DuckDB) ShowTables() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	rows, err := d.db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to show tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// DescribeTable returns the schema for a specific table (dynamic introspection)
func (d *DuckDB) DescribeTable(tableName string) (*TableSchema, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Get column info
	query := fmt.Sprintf("DESCRIBE %s", tableName)
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}
	defer rows.Close()

	schema := &TableSchema{Name: tableName}
	for rows.Next() {
		var colName, colType string
		var nullable, key, defaultVal, extra interface{}
		if err := rows.Scan(&colName, &colType, &nullable, &key, &defaultVal, &extra); err != nil {
			continue
		}
		schema.Columns = append(schema.Columns, ColumnInfo{
			Name: colName,
			Type: colType,
		})
	}

	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	row := d.db.QueryRow(countQuery)
	row.Scan(&schema.RowCount)

	return schema, nil
}

// GetDynamicSchemaContext returns a schema context for AI/LLM consumption
// This is the source of truth - dynamically reads from actual database state
func (d *DuckDB) GetDynamicSchemaContext() (string, error) {
	tables, err := d.ShowTables()
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("## DATA AVAILABILITY - CRITICAL! Only query tables with data:\n\n")

	// Categorize tables
	var tablesWithData, tablesEmpty []TableSchema

	for _, tableName := range tables {
		schema, err := d.DescribeTable(tableName)
		if err != nil {
			continue
		}
		if schema.RowCount > 0 {
			tablesWithData = append(tablesWithData, *schema)
		} else {
			tablesEmpty = append(tablesEmpty, *schema)
		}
	}

	// Tables with data
	sb.WriteString("### Tables WITH data (query these!):\n")
	if len(tablesWithData) > 0 {
		for _, t := range tablesWithData {
			sb.WriteString(fmt.Sprintf("- **%s**: %d rows\n", t.Name, t.RowCount))
		}
	} else {
		sb.WriteString("- None (no data ingested yet)\n")
	}

	// Tables without data
	sb.WriteString("\n### Tables WITHOUT data (DO NOT query these!):\n")
	if len(tablesEmpty) > 0 {
		for _, t := range tablesEmpty {
			sb.WriteString(fmt.Sprintf("- %s: 0 rows - EMPTY, do not use!\n", t.Name))
		}
	}

	// Table schemas
	sb.WriteString("\n## TABLE SCHEMAS:\n\n")
	for _, t := range append(tablesWithData, tablesEmpty...) {
		sb.WriteString(fmt.Sprintf("### Table: %s (%d rows%s)\n",
			t.Name, t.RowCount, func() string {
				if t.RowCount == 0 {
					return " - EMPTY!"
				}
				return ""
			}()))
		sb.WriteString("| Column | Type |\n")
		sb.WriteString("|--------|------|\n")
		for _, col := range t.Columns {
			sb.WriteString(fmt.Sprintf("| %s | %s |\n", col.Name, col.Type))
		}
		sb.WriteString("\n")
	}

	// Add DuckDB-specific syntax guidance
	sb.WriteString(`
## DUCKDB SQL SYNTAX (CRITICAL - NOT SQLite!):
This is DuckDB, NOT SQLite. Use these DuckDB-specific functions:

| Task | DuckDB Syntax (USE THIS) |
|------|--------------------------|
| Convert ms to timestamp | to_timestamp(timestamp / 1000) |
| Current time | now() |
| Time ago | now() - INTERVAL '1 hour' |
| Epoch from timestamp | epoch_ms(now()) |

CRITICAL TIME FILTERING:
- Filter last N hours: WHERE timestamp > epoch_ms(now() - INTERVAL '1 hour') * 1
- epoch_ms() returns BIGINT, timestamp column is also BIGINT

## IMPORTANT - ONLY USE COLUMNS THAT EXIST ABOVE!
If a column is not listed in the table schema above, DO NOT use it in your query.
The tables above show the ACTUAL columns that exist in the database right now.
`)

	return sb.String(), nil
}

// FixColumnNames auto-corrects common LLM hallucinated column names
// This is a safety net - the LLM often generates intuitive but incorrect column names
func (d *DuckDB) FixColumnNames(sql string) string {
	sqlLower := strings.ToLower(sql)
	fixedSQL := sql

	// Check which tables are being queried
	isTracesQuery := strings.Contains(sqlLower, "from spans") || strings.Contains(sqlLower, "join spans")
	isLogsQuery := strings.Contains(sqlLower, "from logs") || strings.Contains(sqlLower, "join logs")
	isMetricsQuery := strings.Contains(sqlLower, "from metrics") || strings.Contains(sqlLower, "join metrics")

	// Skip auto-fix for non-telemetry tables
	isNonTelemetryQuery := strings.Contains(sqlLower, "from inventory") ||
		strings.Contains(sqlLower, "from ownership") ||
		strings.Contains(sqlLower, "from knowledge") ||
		strings.Contains(sqlLower, "from compressed_data") ||
		strings.Contains(sqlLower, "from incident_history")

	if isNonTelemetryQuery {
		return sql
	}

	// Traces/Spans table fixes
	if isTracesQuery {
		tracesFixes := []struct {
			pattern     *regexp.Regexp
			replacement string
		}{
			{regexp.MustCompile(`(?i)\bservice_id\b`), "service"},
			{regexp.MustCompile(`(?i)\boperation_id\b`), "operation"},
			{regexp.MustCompile(`(?i)\bname\b(?=\s*(?:,|FROM|WHERE|GROUP|ORDER|LIMIT|;|\)|$))`), "operation"},
			{regexp.MustCompile(`(?i)\blatency_ms\b`), "(duration_us/1000)"},
			{regexp.MustCompile(`(?i)\blatency\b(?!\s*\()`), "(duration_us/1000)"},
			{regexp.MustCompile(`(?i)\bendpoint\b(?=\s*(?:,|FROM|WHERE|GROUP|ORDER|LIMIT|;|\)|$))`), "operation"},
			{regexp.MustCompile(`(?i)\bparent_service\b`), "service"},
			// status_code → status with value conversions
			{regexp.MustCompile(`(?i)\bstatus_code\s*=\s*2\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bstatus_code\s*>=\s*400\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bstatus_code\s*>=\s*500\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bstatus_code\s*=\s*500\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bstatus_code\b`), "status"},
			// has_error checks
			{regexp.MustCompile(`(?i)\bhas_error\b\s*=\s*true\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bhas_error\b\s*=\s*1\b`), "UPPER(status) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\bhas_error\b(?=\s*(?:,|FROM|WHERE|GROUP|ORDER|LIMIT|;|\)|$|AND|OR))`), "UPPER(status) = 'ERROR'"},
		}
		for _, fix := range tracesFixes {
			fixedSQL = fix.pattern.ReplaceAllString(fixedSQL, fix.replacement)
		}
	}

	// Logs table fixes
	if isLogsQuery {
		logsFixes := []struct {
			pattern     *regexp.Regexp
			replacement string
		}{
			{regexp.MustCompile(`(?i)\bservice_id\b`), "service"},
			// Convert numeric level checks to string level checks
			{regexp.MustCompile(`(?i)\blevel(?:_id)?\s*>=\s*4\b`), "UPPER(level) IN ('ERROR', 'FATAL', 'CRITICAL')"},
			{regexp.MustCompile(`(?i)\blevel(?:_id)?\s*>=\s*3\b`), "UPPER(level) IN ('WARN', 'WARNING', 'ERROR', 'FATAL', 'CRITICAL')"},
			{regexp.MustCompile(`(?i)\blevel(?:_id)?\s*>=\s*2\b`), "UPPER(level) IN ('INFO', 'WARN', 'WARNING', 'ERROR', 'FATAL', 'CRITICAL')"},
			{regexp.MustCompile(`(?i)\blevel(?:_id)?\s*=\s*4\b`), "UPPER(level) = 'ERROR'"},
			{regexp.MustCompile(`(?i)\blevel(?:_id)?\s*=\s*3\b`), "UPPER(level) IN ('WARN', 'WARNING')"},
			{regexp.MustCompile(`(?i)\blevel_id\b`), "level"},
			{regexp.MustCompile(`(?i)\bseverity\b(?=\s*(?:=|>|<|IS|AND|OR|,|FROM|WHERE|GROUP|ORDER|LIMIT|;|\)|$))`), "level"},
		}
		for _, fix := range logsFixes {
			fixedSQL = fix.pattern.ReplaceAllString(fixedSQL, fix.replacement)
		}
	}

	// Metrics table fixes
	if isMetricsQuery {
		metricsFixes := []struct {
			pattern     *regexp.Regexp
			replacement string
		}{
			{regexp.MustCompile(`(?i)\bservice_id\b`), "service"},
			{regexp.MustCompile(`(?i)\bmetric_name\b`), "name"},
		}
		for _, fix := range metricsFixes {
			fixedSQL = fix.pattern.ReplaceAllString(fixedSQL, fix.replacement)
		}
	}

	// Log if we made fixes
	if fixedSQL != sql {
		fmt.Printf("[Storage] Auto-fixed column names in SQL\n")
		fmt.Printf("[Storage] Original: %s\n", truncate(sql, 100))
		fmt.Printf("[Storage] Fixed: %s\n", truncate(fixedSQL, 100))
	}

	return fixedSQL
}

// EnsureColumn dynamically adds a column to a table if it doesn't exist
// This enables schema evolution - new fields in incoming data are automatically added
func (d *DuckDB) EnsureColumn(tableName, columnName, columnType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if column exists
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'", tableName, columnName)
	row := d.db.QueryRow(query)
	var existingCol string
	if err := row.Scan(&existingCol); err == nil {
		// Column already exists
		return nil
	}

	// Add the column
	alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", tableName, columnName, columnType)
	_, err := d.db.Exec(alterQuery)
	if err != nil {
		return fmt.Errorf("failed to add column %s to %s: %w", columnName, tableName, err)
	}

	fmt.Printf("[Storage] Dynamically added column %s (%s) to table %s\n", columnName, columnType, tableName)
	return nil
}

// EnsureTableWithSchema creates a table if it doesn't exist, using the provided schema
// This enables dynamic table creation for new data types
func (d *DuckDB) EnsureTableWithSchema(tableName string, columns map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Build CREATE TABLE statement
	var colDefs []string
	for name, colType := range columns {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", name, colType))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, strings.Join(colDefs, ", "))
	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

// InferColumnType infers the DuckDB column type from a Go value
func InferColumnType(value interface{}) string {
	switch value.(type) {
	case int, int32, int64:
		return "BIGINT"
	case float32, float64:
		return "DOUBLE"
	case bool:
		return "BOOLEAN"
	case string:
		return "VARCHAR"
	case map[string]interface{}, map[string]string, []interface{}:
		return "JSON"
	default:
		return "VARCHAR"
	}
}

// GetTableRowCount returns the row count for a specific table
func (d *DuckDB) GetTableRowCount(tableName string) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	row := d.db.QueryRow(query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// GetDistinctValues returns distinct values for a column (useful for data introspection)
func (d *DuckDB) GetDistinctValues(tableName, columnName string, limit int) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL LIMIT %d",
		columnName, tableName, columnName, limit)
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err == nil {
			values = append(values, val)
		}
	}
	return values, nil
}

// IntrospectDataValues returns actual data values from the database for AI context
// This prevents the AI from guessing values - it uses real data from the database
func (d *DuckDB) IntrospectDataValues(query string) (map[string][]string, error) {
	queryLower := strings.ToLower(query)
	result := make(map[string][]string)

	// Inventory introspection
	if strings.Contains(queryLower, "service") || strings.Contains(queryLower, "status") ||
		strings.Contains(queryLower, "inventory") || strings.Contains(queryLower, "production") {
		if vals, err := d.GetDistinctValues("spans", "service", 20); err == nil && len(vals) > 0 {
			result["spans.service"] = vals
		}
		if vals, err := d.GetDistinctValues("logs", "service", 20); err == nil && len(vals) > 0 {
			result["logs.service"] = vals
		}
	}

	// Log level introspection
	if strings.Contains(queryLower, "error") || strings.Contains(queryLower, "log") ||
		strings.Contains(queryLower, "level") {
		if vals, err := d.GetDistinctValues("logs", "level", 10); err == nil && len(vals) > 0 {
			result["logs.level"] = vals
		}
	}

	// Status introspection
	if strings.Contains(queryLower, "status") || strings.Contains(queryLower, "error") {
		if vals, err := d.GetDistinctValues("spans", "status", 10); err == nil && len(vals) > 0 {
			result["spans.status"] = vals
		}
	}

	// Operation introspection
	if strings.Contains(queryLower, "endpoint") || strings.Contains(queryLower, "operation") ||
		strings.Contains(queryLower, "api") {
		if vals, err := d.GetDistinctValues("spans", "operation", 20); err == nil && len(vals) > 0 {
			result["spans.operation"] = vals
		}
	}

	return result, nil
}

// truncate helper for logging
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
