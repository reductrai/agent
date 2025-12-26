package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/reductrai/agent/internal/storage"
	"github.com/spf13/cobra"
)

var (
	version    = "0.4.1"
	cloudURL   = "https://app.reductrai.com"
	license    string
	port       int
	dataDir    string
	bufferSize int
	batchSize  int
)

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func getDefaultDataDir() string {
	// Check environment variable first
	if v := os.Getenv("REDUCTRAI_DATA_DIR"); v != "" {
		return v
	}
	// Default to ~/.reductrai (no sudo required)
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".reductrai")
	}
	// Fallback
	return ".reductrai"
}

// PID file management
func getPidFile() string {
	return filepath.Join(getDefaultDataDir(), "reductrai.pid")
}

func writePidFile() error {
	pidFile := getPidFile()
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(pidFile), 0755); err != nil {
		return err
	}
	return os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644)
}

func readPidFile() (int, error) {
	data, err := os.ReadFile(getPidFile())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

func removePidFile() {
	os.Remove(getPidFile())
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "reductrai",
		Short: "ReductrAI - Autonomous Infrastructure Intelligence",
		Long: `ReductrAI binary runs locally on your infrastructure.

  - Ingests 100% of your telemetry (metrics, logs, traces)
  - Stores everything locally in DuckDB (your data never leaves)
  - Detects anomalies automatically (FREE - no LLM needed)
  - Sends tiny summaries (~2KB) to cloud for AI reasoning
  - Receives remediation commands after human approval`,
	}

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&license, "license", "l", os.Getenv("REDUCTRAI_LICENSE"), "License key")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "p", getEnvInt("REDUCTRAI_PORT", 8080), "Port for telemetry ingestion")
	rootCmd.PersistentFlags().StringVarP(&dataDir, "data-dir", "d", getDefaultDataDir(), "Data directory for DuckDB")

	// Advanced flags (hidden - auto-scales by default)
	rootCmd.PersistentFlags().IntVar(&bufferSize, "buffer-size", getEnvInt("REDUCTRAI_BUFFER_SIZE", 100000), "Ingestion buffer size")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", getEnvInt("REDUCTRAI_BATCH_SIZE", 1000), "Initial batch size (auto-scales)")
	rootCmd.PersistentFlags().MarkHidden("buffer-size")
	rootCmd.PersistentFlags().MarkHidden("batch-size")

	// Commands
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(stopCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(versionCmd())
	rootCmd.AddCommand(queryCmd())
	rootCmd.AddCommand(schemaCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func startCmd() *cobra.Command {
	var localMode bool

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the ReductrAI agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check if already running
			if pid, err := readPidFile(); err == nil {
				if isProcessRunning(pid) {
					fmt.Printf("%s ReductrAI is already running (PID %d)%s\n", ColorYellow, pid, ColorReset)
					fmt.Printf("  Use %sreductrai stop%s to stop it first.\n", ColorCyan, ColorReset)
					return nil
				}
				// Stale PID file, remove it
				removePidFile()
			}

			// Determine mode: local-only (no license) vs cloud (with license)
			// Local mode is FREE and unlimited - all local features work
			// Cloud mode requires license - enables dashboard and AI relay
			if license == "" {
				localMode = true
			}

			if localMode {
				license = "local-mode"
			}

			fmt.Println()
			fmt.Println(ColorCyan + "╔═══════════════════════════════════════════════════════════╗")
			fmt.Println("║                      ReductrAI                            ║")
			fmt.Println("║           Autonomous Infrastructure Intelligence          ║")
			fmt.Println("╚═══════════════════════════════════════════════════════════╝" + ColorReset)
			fmt.Printf("\n  %sVersion:%s     %s\n", ColorGray, ColorReset, version)
			if localMode {
				fmt.Printf("  %sMode:%s        %sLOCAL ONLY%s (FREE - no license required)\n", ColorGray, ColorReset, ColorGreen, ColorReset)
				fmt.Printf("  %sCloud:%s       %sDISABLED%s (use --license for cloud features)\n", ColorGray, ColorReset, ColorYellow, ColorReset)
			} else {
				fmt.Printf("  %sLicense:%s     %s...\n", ColorGray, ColorReset, license[:8])
				fmt.Printf("  %sCloud:%s       %s\n", ColorGray, ColorReset, cloudURL)
			}
			fmt.Printf("  %sPort:%s        %d\n", ColorGray, ColorReset, port)
			fmt.Printf("  %sData Dir:%s    %s\n\n", ColorGray, ColorReset, dataDir)

			// Write PID file
			if err := writePidFile(); err != nil {
				fmt.Printf("  %sWarning:%s Could not write PID file: %v\n", ColorYellow, ColorReset, err)
			}

			// Start the agent
			agent, err := NewAgent(AgentConfig{
				License:    license,
				Port:       port,
				DataDir:    dataDir,
				CloudURL:   cloudURL,
				LocalMode:  localMode,
				BufferSize: bufferSize,
				BatchSize:  batchSize,
			})
			if err != nil {
				removePidFile()
				return fmt.Errorf("failed to create agent: %w", err)
			}

			// Run agent (blocks until shutdown)
			err = agent.Run()
			removePidFile()
			return err
		},
	}

	cmd.Flags().BoolVar(&localMode, "local", false, "Local-only mode (no cloud features, no license required)")
	return cmd
}

func stopCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the ReductrAI agent",
		Run: func(cmd *cobra.Command, args []string) {
			pid, err := readPidFile()
			if err != nil {
				fmt.Printf("%s✗ ReductrAI is not running%s (no PID file found)\n", ColorYellow, ColorReset)
				return
			}

			if !isProcessRunning(pid) {
				fmt.Printf("%s✗ ReductrAI is not running%s (stale PID file)\n", ColorYellow, ColorReset)
				removePidFile()
				return
			}

			process, err := os.FindProcess(pid)
			if err != nil {
				fmt.Printf("%s✗ Could not find process %d%s\n", ColorRed, pid, ColorReset)
				return
			}

			fmt.Printf("  Stopping ReductrAI (PID %d)... ", pid)

			// Send SIGTERM for graceful shutdown
			signal := syscall.SIGTERM
			if force {
				signal = syscall.SIGKILL
			}

			if err := process.Signal(signal); err != nil {
				fmt.Printf("%sFAILED%s\n", ColorRed, ColorReset)
				fmt.Printf("  Error: %v\n", err)
				return
			}

			// Wait for process to exit (with timeout)
			for i := 0; i < 30; i++ { // 3 second timeout
				time.Sleep(100 * time.Millisecond)
				if !isProcessRunning(pid) {
					fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)
					removePidFile()
					fmt.Printf("\n%s✓ ReductrAI stopped successfully%s\n", ColorGreen, ColorReset)
					return
				}
			}

			// Process still running, try SIGKILL
			if !force {
				fmt.Printf("%stimeout%s\n", ColorYellow, ColorReset)
				fmt.Printf("  Sending SIGKILL... ")
				process.Signal(syscall.SIGKILL)
				fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)
			}
			removePidFile()
			fmt.Printf("\n%s✓ ReductrAI stopped%s\n", ColorGreen, ColorReset)
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force stop with SIGKILL")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show agent status",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("%s", ColorCyan)
			fmt.Println("╔═══════════════════════════════════════════════════════════╗")
			fmt.Println("║                  ReductrAI Status                         ║")
			fmt.Println("╚═══════════════════════════════════════════════════════════╝")
			fmt.Printf("%s\n", ColorReset)

			pid, err := readPidFile()
			if err != nil {
				fmt.Printf("  %sStatus:%s      %s● Stopped%s\n", ColorGray, ColorReset, ColorRed, ColorReset)
				fmt.Printf("  %sData Dir:%s    %s\n", ColorGray, ColorReset, getDefaultDataDir())
				return
			}

			if !isProcessRunning(pid) {
				fmt.Printf("  %sStatus:%s      %s● Stopped%s (stale PID)\n", ColorGray, ColorReset, ColorYellow, ColorReset)
				fmt.Printf("  %sData Dir:%s    %s\n", ColorGray, ColorReset, getDefaultDataDir())
				removePidFile()
				return
			}

			fmt.Printf("  %sStatus:%s      %s● Running%s\n", ColorGray, ColorReset, ColorGreen, ColorReset)
			fmt.Printf("  %sPID:%s         %d\n", ColorGray, ColorReset, pid)
			fmt.Printf("  %sData Dir:%s    %s\n", ColorGray, ColorReset, getDefaultDataDir())
			fmt.Printf("  %sEndpoint:%s    http://localhost:%d\n", ColorGray, ColorReset, port)
			fmt.Printf("\n  Use %sreductrai stop%s to stop the agent.\n", ColorCyan, ColorReset)
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("reductrai version %s\n", version)
		},
	}
}

func queryCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "query [sql]",
		Short: "Run a local DuckDB query",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := storage.NewDuckDB(dataDir)
			if err != nil {
				return fmt.Errorf("failed to open database: %w", err)
			}
			defer store.Close()

			rows, err := store.Query(args[0])
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			defer rows.Close()

			// Get column names
			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			// Print header
			for i, col := range cols {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(col)
			}
			fmt.Println()

			// Print rows
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			for rows.Next() {
				if err := rows.Scan(valuePtrs...); err != nil {
					continue
				}
				for i, v := range values {
					if i > 0 {
						fmt.Print("\t")
					}
					fmt.Printf("%v", v)
				}
				fmt.Println()
			}

			return nil
		},
	}
}

func schemaCmd() *cobra.Command {
	var dynamic bool

	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Show database schema and example queries",
		Run: func(cmd *cobra.Command, args []string) {
			if dynamic {
				// Show dynamic schema from actual database
				showDynamicSchema()
				return
			}

			// Show static schema documentation
			fmt.Println(`
╔═══════════════════════════════════════════════════════════╗
║                  ReductrAI Database Schema                ║
╚═══════════════════════════════════════════════════════════╝

TABLES
──────

metrics
  timestamp    TIMESTAMP   When the metric was recorded
  service      VARCHAR     Service name (e.g., "api-gateway")
  name         VARCHAR     Metric name (e.g., "http_requests_total")
  value        DOUBLE      Metric value
  tags         JSON        Additional labels {"env": "prod", ...}
  tier         VARCHAR     Storage tier (hot/warm/cold)

logs
  timestamp    TIMESTAMP   When the log was recorded
  service      VARCHAR     Service name
  level        VARCHAR     Log level (INFO, WARN, ERROR, etc.)
  message      VARCHAR     Log message
  tags         JSON        Additional context
  tier         VARCHAR     Storage tier

spans (traces)
  timestamp      TIMESTAMP   Span start time
  trace_id       VARCHAR     Trace ID (links related spans)
  span_id        VARCHAR     Unique span ID
  parent_span_id VARCHAR     Parent span (for call hierarchy)
  service        VARCHAR     Service name
  operation      VARCHAR     Operation name (e.g., "GET /users")
  duration_us    BIGINT      Duration in microseconds
  status         VARCHAR     Status (OK, ERROR)
  tags           JSON        Span attributes
  tier           VARCHAR     Storage tier

EXAMPLE QUERIES
───────────────

# Count errors by service (last hour)
reductrai query "SELECT service, COUNT(*) as errors FROM spans WHERE status='ERROR' AND timestamp > now() - interval '1 hour' GROUP BY service ORDER BY errors DESC"

# Top 10 slowest operations
reductrai query "SELECT service, operation, AVG(duration_us)/1000 as avg_ms FROM spans GROUP BY service, operation ORDER BY avg_ms DESC LIMIT 10"

# Error rate by service
reductrai query "SELECT service, COUNT(*) as total, SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as errors, ROUND(100.0 * SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_pct FROM spans WHERE timestamp > now() - interval '1 hour' GROUP BY service"

# Recent error logs
reductrai query "SELECT timestamp, service, message FROM logs WHERE level IN ('ERROR', 'error') ORDER BY timestamp DESC LIMIT 20"

# Log volume by level
reductrai query "SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC"

# Service dependencies (which services call which)
reductrai query "SELECT DISTINCT parent.service as caller, child.service as callee FROM spans parent JOIN spans child ON parent.span_id = child.parent_span_id WHERE parent.service != child.service"

# Storage stats by tier
reductrai query "SELECT 'spans' as table_name, tier, COUNT(*) as rows FROM spans GROUP BY tier UNION ALL SELECT 'logs', tier, COUNT(*) FROM logs GROUP BY tier UNION ALL SELECT 'metrics', tier, COUNT(*) FROM metrics GROUP BY tier"

# Latency percentiles by service
reductrai query "SELECT service, PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_us)/1000 as p50_ms, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_us)/1000 as p95_ms, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_us)/1000 as p99_ms FROM spans GROUP BY service"

NOTE: All queries are FREE and unlimited. Only AI investigations count toward your plan.

TIP: Use 'reductrai schema --dynamic' to see the actual schema from your database.`)
		},
	}

	cmd.Flags().BoolVarP(&dynamic, "dynamic", "d", false, "Show actual schema from database (includes row counts)")
	return cmd
}

// showDynamicSchema displays the schema dynamically from the database
func showDynamicSchema() {
	dataDir := os.Getenv("REDUCTRAI_DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join(os.Getenv("HOME"), ".reductrai")
	}

	db, err := storage.NewDuckDB(dataDir)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}
	defer db.Close()

	fmt.Println(`
╔═══════════════════════════════════════════════════════════╗
║            ReductrAI Dynamic Database Schema              ║
╚═══════════════════════════════════════════════════════════╝`)

	// Get dynamic schema context
	schemaContext, err := db.GetDynamicSchemaContext()
	if err != nil {
		fmt.Printf("Error getting schema: %v\n", err)
		return
	}

	fmt.Println(schemaContext)

	// Also show data introspection for common queries
	fmt.Println("\n## ACTUAL DATA VALUES (sample from your database):")

	introspection, _ := db.IntrospectDataValues("service status level operation")
	for key, values := range introspection {
		if len(values) > 0 {
			fmt.Printf("**%s**: %s\n", key, strings.Join(values, ", "))
		}
	}

	fmt.Println("\nNOTE: This is the ACTUAL schema from your database. Use this for accurate queries.")
}
