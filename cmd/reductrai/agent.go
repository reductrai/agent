package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/reductrai/agent/internal/cloud"
	ctxpkg "github.com/reductrai/agent/internal/context"
	"github.com/reductrai/agent/internal/detector"
	"github.com/reductrai/agent/internal/ingest"
	"github.com/reductrai/agent/internal/storage"
	"github.com/reductrai/agent/pkg/types"
)

type AgentConfig struct {
	License    string
	Port       int
	DataDir    string
	CloudURL   string
	LocalMode  bool // Local-only mode: no cloud, no license required (FREE)
	BufferSize int  // Ingestion buffer size (default 100K)
	BatchSize  int  // Batch insert size (default 1000)
}

// detectTierFromLicense extracts tier from license key format: REDUCTRAI-TIER-XXX
func detectTierFromLicense(license string) string {
	// License format: REDUCTRAI-TIER-XXX or RF-TIER-XXX
	// Examples: REDUCTRAI-ENT-XXX, RF-PRO-XXX, RF-BUS-XXX
	tierCodes := map[string]string{
		"ENT":        "ENTERPRISE",
		"ENTERPRISE": "ENTERPRISE",
		"BUS":        "BUSINESS",
		"BUSINESS":   "BUSINESS",
		"PRO":        "PRO",
		"FREE":       "FREE",
	}

	// Check for tier codes in license
	for code, tier := range tierCodes {
		if len(license) >= len(code) {
			// Check if code appears after first dash
			for i := 0; i < len(license)-len(code); i++ {
				if license[i] == '-' && i+1+len(code) <= len(license) {
					segment := license[i+1 : i+1+len(code)]
					if segment == code {
						return tier
					}
				}
			}
		}
	}

	return "FREE"
}

type Agent struct {
	config    AgentConfig
	storage   *storage.DuckDB
	ingester  *ingest.Server
	detector  *detector.AnomalyDetector
	cloud     *cloud.Client
	extractor *ctxpkg.Extractor
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewAgent(config AgentConfig) (*Agent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Detect license tier from license key
	licenseTier := detectTierFromLicense(config.License)

	// Initialize DuckDB storage with tier-based retention
	store, err := storage.NewDuckDBWithTier(config.DataDir, licenseTier)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	fmt.Printf("  Tier:        %s (retention: %d days)\n",
		licenseTier, int(store.GetRetentionPeriod().Hours()/24))

	// Initialize cloud client
	cloudClient := cloud.NewClient(config.CloudURL, config.License)

	// Initialize anomaly detector
	detect := detector.New(store)

	// Initialize ingestion server with buffering config
	var ingester *ingest.Server
	if config.BufferSize > 0 || config.BatchSize > 0 {
		ingester = ingest.NewServerWithConfig(config.Port, store, config.BufferSize, config.BatchSize)
	} else {
		ingester = ingest.NewServer(config.Port, store)
	}

	// Initialize context extractor for enriched context extraction
	extractor := ctxpkg.NewExtractor(store.GetDB())

	return &Agent{
		config:    config,
		storage:   store,
		ingester:  ingester,
		detector:  detect,
		cloud:     cloudClient,
		extractor: extractor,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (a *Agent) Run() error {
	fmt.Println("Starting ReductrAI agent...")

	// Validate license with cloud (skip in local mode)
	if a.config.LocalMode {
		fmt.Printf("  License validation... %sSKIPPED%s (local mode)\n", ColorYellow, ColorReset)
	} else {
		fmt.Print("  Validating license... ")
		if err := a.cloud.ValidateLicense(); err != nil {
			fmt.Printf("%sFAILED%s\n", ColorRed, ColorReset)
			return fmt.Errorf("license validation failed: %w", err)
		}
		fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)
	}

	// Start ingestion server
	fmt.Print("  Starting ingestion server... ")
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		if err := a.ingester.Start(a.ctx); err != nil {
			fmt.Printf("%s[Server] Error: %v%s\n", ColorRed, err, ColorReset)
		}
	}()
	fmt.Printf("%sOK%s (port %d)\n", ColorGreen, ColorReset, a.config.Port)

	// Start anomaly detection loop
	fmt.Print("  Starting anomaly detector... ")
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.runDetectionLoop()
	}()
	fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)

	// Start cloud sync loop (skip in local mode)
	if a.config.LocalMode {
		fmt.Printf("  Cloud sync... %sSKIPPED%s (local mode)\n", ColorYellow, ColorReset)
		fmt.Printf("  Command listener... %sSKIPPED%s (local mode)\n", ColorYellow, ColorReset)
	} else {
		fmt.Print("  Connecting to cloud... ")
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.runCloudSync()
		}()
		fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)

		// Start WebSocket listener for remediation commands
		fmt.Print("  Starting command listener... ")
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.cloud.ListenForCommands(a.ctx, a.handleRemediationCommand)
		}()
		fmt.Printf("%sOK%s\n", ColorGreen, ColorReset)
	}

	fmt.Printf("\n  %s✓ ReductrAI is running.%s Press Ctrl+C to stop.\n\n", ColorGreen, ColorReset)
	fmt.Printf("  %sIngestion:%s    http://localhost:%d\n", ColorGray, ColorReset, a.config.Port)
	fmt.Printf("  %sFormats:%s      OTLP, Prometheus, Datadog, and 35+ more\n", ColorGray, ColorReset)
	fmt.Printf("  %sQueries:%s      reductrai query \"SELECT * FROM spans LIMIT 10\"\n", ColorGray, ColorReset)
	if a.config.LocalMode {
		fmt.Printf("\n  %s💡 Local Mode Tips:%s\n", ColorCyan, ColorReset)
		fmt.Printf("     • Use your own LLM: set LLM_ENDPOINT=http://localhost:11434/v1\n")
		fmt.Printf("     • All data stays local in DuckDB\n")
		fmt.Printf("     • Add a license for cloud dashboard: --license YOUR_KEY\n")
	}
	fmt.Println()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	fmt.Printf("\n%s[%s] Shutting down gracefully...%s\n", ColorYellow, sig, ColorReset)
	a.cancel()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(5 * time.Second):
		fmt.Printf("%s  Timeout waiting for goroutines, forcing shutdown%s\n", ColorYellow, ColorReset)
	}

	a.storage.Close()
	fmt.Printf("%s✓ ReductrAI stopped.%s\n", ColorGreen, ColorReset)
	return nil
}

// runDetectionLoop runs anomaly detection every 10 seconds
func (a *Agent) runDetectionLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			anomalies := a.detector.Detect()
			if len(anomalies) > 0 {
				fmt.Printf("[Detector] Found %d anomalies\n", len(anomalies))

				// Extract and send enriched context for each anomaly (async)
				// In local mode, anomalies are logged but not sent to cloud
				if !a.config.LocalMode {
					for _, anomaly := range anomalies {
						go a.sendEnrichedContext(anomaly)
					}
				}
			}
		}
	}
}

// sendEnrichedContext extracts enriched context for an anomaly and sends to cloud
func (a *Agent) sendEnrichedContext(anomaly types.Anomaly) {
	enrichedCtx, err := a.extractor.Extract(anomaly)
	if err != nil {
		fmt.Printf("[Context] Failed to extract context for %s: %v\n", anomaly.Service, err)
		return
	}

	// Log context size
	size := enrichedCtx.EstimateSize()
	fmt.Printf("[Context] Extracted enriched context for %s (~%d bytes)\n", anomaly.Service, size)

	// Send to cloud
	investigationID, err := a.cloud.SendEnrichedContext(enrichedCtx)
	if err != nil {
		fmt.Printf("[Context] Failed to send context: %v\n", err)
		return
	}

	if investigationID != "" {
		fmt.Printf("[Context] Investigation started: %s\n", investigationID)
	}
}

// runCloudSync sends summaries to cloud every 30 seconds
func (a *Agent) runCloudSync() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			summary := a.buildSummary()
			if err := a.cloud.SendSummary(summary); err != nil {
				fmt.Printf("[Cloud] Failed to send summary: %v\n", err)
			}
		}
	}
}

// buildSummary aggregates current state into ~2KB summary
func (a *Agent) buildSummary() *cloud.Summary {
	services := a.storage.GetServiceHealth()
	anomalies := a.detector.GetActiveAnomalies()

	healthy := 0
	degraded := 0
	for _, svc := range services {
		if svc.ErrorRate < 0.01 {
			healthy++
		} else {
			degraded++
		}
	}

	return &cloud.Summary{
		CollectedAt:      time.Now().Unix(),
		Services:         services,
		TotalServices:    len(services),
		HealthyServices:  healthy,
		DegradedServices: degraded,
		Anomalies:        anomalies,
	}
}

// handleRemediationCommand executes commands from cloud after human approval
func (a *Agent) handleRemediationCommand(cmd cloud.RemediationCommand) {
	fmt.Printf("[Remediation] Executing: %s\n", cmd.Title)

	// Check for custom LLM-generated command
	if cmd.Command != "" {
		fmt.Printf("[Remediation] Running custom command: %s\n", cmd.Command)
		output, err := a.executeShellCommand(cmd.Command)
		if err != nil {
			fmt.Printf("[Remediation] Command failed: %v\n", err)
			a.cloud.ReportCommandResult(cmd.ID, "failed", err.Error())
			return
		}
		// Truncate output if too long
		if len(output) > 500 {
			output = output[:500] + "... (truncated)"
		}
		fmt.Printf("[Remediation] Command output: %s\n", output)
		a.cloud.ReportCommandResult(cmd.ID, "completed", "")
		return
	}

	// Legacy template-based execution
	switch cmd.Type {
	case "scale":
		fmt.Printf("  Scaling %s to %d replicas\n", cmd.Target, cmd.Replicas)
		// TODO: Execute kubectl or API call
	case "restart":
		fmt.Printf("  Restarting %s\n", cmd.Target)
		// TODO: Execute restart
	case "circuit_breaker":
		fmt.Printf("  Enabling circuit breaker for %s\n", cmd.Target)
		// TODO: Update config
	default:
		fmt.Printf("  Unknown action type: %s\n", cmd.Type)
	}

	// Report result back to cloud
	a.cloud.ReportCommandResult(cmd.ID, "completed", "")
}

// executeShellCommand runs a shell command with timeout
func (a *Agent) executeShellCommand(command string) (string, error) {
	// Create context with 60 second timeout
	ctx, cancel := context.WithTimeout(a.ctx, 60*time.Second)
	defer cancel()

	// Execute command in a shell
	cmd := exec.CommandContext(ctx, "sh", "-c", command)

	// Capture combined stdout/stderr
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("command failed: %w (output: %s)", err, string(output))
	}

	return string(output), nil
}
