package infra

import (
	"context"
	"os"
	"strconv"
	"strings"
)

// CollectDatabase gathers database state (call separately as needed)
func (c *Collector) CollectDatabase(ctx context.Context, serviceName string) *DatabaseState {
	// Try to detect database type from service name or environment
	dbType := detectDatabaseType(serviceName)
	if dbType == "" {
		return nil
	}

	state := &DatabaseState{
		Type:      dbType,
		Available: true,
	}

	switch dbType {
	case "postgres":
		c.collectPostgresState(ctx, state)
	case "mysql":
		c.collectMySQLState(ctx, state)
	case "mongodb":
		c.collectMongoState(ctx, state)
	}

	return state
}

// CollectCache gathers cache state (call separately as needed)
func (c *Collector) CollectCache(ctx context.Context, serviceName string) *CacheState {
	// Try to detect cache type from service name
	cacheType := detectCacheType(serviceName)
	if cacheType == "" {
		return nil
	}

	state := &CacheState{
		Type:      cacheType,
		Available: true,
	}

	switch cacheType {
	case "redis":
		c.collectRedisState(ctx, state)
	case "memcached":
		c.collectMemcachedState(ctx, state)
	}

	return state
}

func detectDatabaseType(serviceName string) string {
	name := strings.ToLower(serviceName)
	switch {
	case strings.Contains(name, "postgres") || strings.Contains(name, "pg"):
		return "postgres"
	case strings.Contains(name, "mysql") || strings.Contains(name, "mariadb"):
		return "mysql"
	case strings.Contains(name, "mongo"):
		return "mongodb"
	default:
		return ""
	}
}

func detectCacheType(serviceName string) string {
	name := strings.ToLower(serviceName)
	switch {
	case strings.Contains(name, "redis"):
		return "redis"
	case strings.Contains(name, "memcache"):
		return "memcached"
	default:
		return ""
	}
}

// collectPostgresState collects PostgreSQL connection and performance stats
func (c *Collector) collectPostgresState(ctx context.Context, state *DatabaseState) {
	// Check if psql is available
	if _, err := c.execCommand(ctx, "which", "psql"); err != nil {
		state.Available = false
		return
	}

	// Get connection string from environment
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		// Try individual vars
		host := getEnvOrDefault("PGHOST", "localhost")
		port := getEnvOrDefault("PGPORT", "5432")
		user := getEnvOrDefault("PGUSER", "postgres")
		db := getEnvOrDefault("PGDATABASE", "postgres")
		connStr = "postgres://" + user + "@" + host + ":" + port + "/" + db
	}

	// Query connection stats
	query := `SELECT
		(SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active,
		(SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle,
		(SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max,
		(SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock') as waiting`

	output, err := c.execCommand(ctx, "psql", connStr, "-t", "-c", query)
	if err != nil {
		state.Available = false
		return
	}

	// Parse output: "active | idle | max | waiting"
	parts := strings.Split(strings.TrimSpace(output), "|")
	if len(parts) >= 4 {
		state.ActiveConnections, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
		state.IdleConnections, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
		state.MaxConnections, _ = strconv.Atoi(strings.TrimSpace(parts[2]))
		state.WaitingConnections, _ = strconv.Atoi(strings.TrimSpace(parts[3]))
	}

	// Get cache hit ratio
	cacheQuery := `SELECT
		round(100.0 * sum(blks_hit) / nullif(sum(blks_hit) + sum(blks_read), 0), 2)
		FROM pg_stat_database`
	if output, err := c.execCommand(ctx, "psql", connStr, "-t", "-c", cacheQuery); err == nil {
		if ratio, err := strconv.ParseFloat(strings.TrimSpace(output), 64); err == nil {
			state.CacheHitRatio = ratio
		}
	}

	// Get database size
	sizeQuery := `SELECT pg_size_pretty(pg_database_size(current_database()))`
	if output, err := c.execCommand(ctx, "psql", connStr, "-t", "-c", sizeQuery); err == nil {
		state.DatabaseSize = strings.TrimSpace(output)
	}

	// Check replication status if replica
	repQuery := `SELECT
		CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END,
		COALESCE(pg_last_wal_receive_lsn()::text, 'n/a')`
	if output, err := c.execCommand(ctx, "psql", connStr, "-t", "-c", repQuery); err == nil {
		parts := strings.Split(strings.TrimSpace(output), "|")
		if len(parts) >= 1 {
			state.ReplicationState = strings.TrimSpace(parts[0])
		}
	}
}

// collectMySQLState collects MySQL connection stats
func (c *Collector) collectMySQLState(ctx context.Context, state *DatabaseState) {
	// Check if mysql is available
	if _, err := c.execCommand(ctx, "which", "mysql"); err != nil {
		state.Available = false
		return
	}

	host := getEnvOrDefault("MYSQL_HOST", "localhost")
	user := getEnvOrDefault("MYSQL_USER", "root")

	// Query connection stats
	query := `SELECT
		(SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep') as active,
		(SELECT COUNT(*) FROM information_schema.processlist WHERE command = 'Sleep') as idle,
		(SELECT @@max_connections) as max_conn`

	output, err := c.execCommand(ctx, "mysql", "-h", host, "-u", user, "-N", "-e", query)
	if err != nil {
		state.Available = false
		return
	}

	// Parse tab-separated output
	parts := strings.Fields(strings.TrimSpace(output))
	if len(parts) >= 3 {
		state.ActiveConnections, _ = strconv.Atoi(parts[0])
		state.IdleConnections, _ = strconv.Atoi(parts[1])
		state.MaxConnections, _ = strconv.Atoi(parts[2])
	}
}

// collectMongoState collects MongoDB stats
func (c *Collector) collectMongoState(ctx context.Context, state *DatabaseState) {
	// Check if mongosh or mongo is available
	mongoCli := "mongosh"
	if _, err := c.execCommand(ctx, "which", "mongosh"); err != nil {
		mongoCli = "mongo"
		if _, err := c.execCommand(ctx, "which", "mongo"); err != nil {
			state.Available = false
			return
		}
	}

	uri := getEnvOrDefault("MONGODB_URI", "mongodb://localhost:27017")

	// Get server status
	query := `db.serverStatus().connections`
	output, err := c.execCommand(ctx, mongoCli, uri, "--eval", query, "--quiet")
	if err != nil {
		state.Available = false
		return
	}

	// Parse JSON-like output for current connections
	if strings.Contains(output, "current") {
		// Simple parsing - look for "current" : N
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "current") {
				parts := strings.Split(line, ":")
				if len(parts) >= 2 {
					numStr := strings.Trim(parts[1], " ,")
					state.ActiveConnections, _ = strconv.Atoi(numStr)
				}
			}
		}
	}
}

// collectRedisState collects Redis stats
func (c *Collector) collectRedisState(ctx context.Context, state *CacheState) {
	// Check if redis-cli is available
	if _, err := c.execCommand(ctx, "which", "redis-cli"); err != nil {
		state.Available = false
		return
	}

	host := getEnvOrDefault("REDIS_HOST", "localhost")
	port := getEnvOrDefault("REDIS_PORT", "6379")

	// Get INFO output
	output, err := c.execCommand(ctx, "redis-cli", "-h", host, "-p", port, "INFO")
	if err != nil {
		state.Available = false
		return
	}

	info := parseRedisInfo(output)

	// Memory
	state.UsedMemory = info["used_memory_human"]
	state.MaxMemory = info["maxmemory_human"]
	if usedBytes, err := strconv.ParseInt(info["used_memory"], 10, 64); err == nil {
		if maxBytes, err := strconv.ParseInt(info["maxmemory"], 10, 64); err == nil && maxBytes > 0 {
			state.MemoryPercent = float64(usedBytes) / float64(maxBytes) * 100
		}
	}

	// Hit rate
	hits, _ := strconv.ParseInt(info["keyspace_hits"], 10, 64)
	misses, _ := strconv.ParseInt(info["keyspace_misses"], 10, 64)
	if hits+misses > 0 {
		state.HitRate = float64(hits) / float64(hits+misses) * 100
		state.MissRate = float64(misses) / float64(hits+misses) * 100
	}

	// Evictions
	state.EvictedKeys, _ = strconv.ParseInt(info["evicted_keys"], 10, 64)

	// Connections
	state.ConnectedClients, _ = strconv.Atoi(info["connected_clients"])
	state.BlockedClients, _ = strconv.Atoi(info["blocked_clients"])

	// Replication
	state.Role = info["role"]
	if state.Role == "slave" {
		state.ReplicationLag, _ = strconv.Atoi(info["master_link_down_since_seconds"])
	}
}

// collectMemcachedState collects Memcached stats
func (c *Collector) collectMemcachedState(ctx context.Context, state *CacheState) {
	// Check if we can connect via nc or memcached tools
	host := getEnvOrDefault("MEMCACHED_HOST", "localhost")
	port := getEnvOrDefault("MEMCACHED_PORT", "11211")

	// Try using nc to send stats command
	output, err := c.execCommand(ctx, "sh", "-c",
		"echo 'stats' | nc -q 1 "+host+" "+port)
	if err != nil {
		state.Available = false
		return
	}

	stats := parseMemcachedStats(output)

	// Memory
	if bytes, ok := stats["bytes"]; ok {
		if b, err := strconv.ParseInt(bytes, 10, 64); err == nil {
			state.UsedMemory = formatBytes(b)
		}
	}
	if maxBytes, ok := stats["limit_maxbytes"]; ok {
		if b, err := strconv.ParseInt(maxBytes, 10, 64); err == nil {
			state.MaxMemory = formatBytes(b)
		}
	}

	// Hit rate
	hits, _ := strconv.ParseInt(stats["get_hits"], 10, 64)
	misses, _ := strconv.ParseInt(stats["get_misses"], 10, 64)
	if hits+misses > 0 {
		state.HitRate = float64(hits) / float64(hits+misses) * 100
	}

	// Evictions
	state.EvictedKeys, _ = strconv.ParseInt(stats["evictions"], 10, 64)

	// Connections
	state.ConnectedClients, _ = strconv.Atoi(stats["curr_connections"])
}

func parseRedisInfo(output string) map[string]string {
	info := make(map[string]string)
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			info[parts[0]] = parts[1]
		}
	}
	return info
}

func parseMemcachedStats(output string) map[string]string {
	stats := make(map[string]string)
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "STAT ") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				stats[parts[1]] = parts[2]
			}
		}
	}
	return stats
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
