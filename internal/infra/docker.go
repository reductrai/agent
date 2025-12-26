package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// collectDocker gathers Docker container state for a service
func (c *Collector) collectDocker(ctx context.Context, serviceName string) *DockerState {
	if !c.hasDocker {
		return nil
	}

	state := &DockerState{
		Available: true,
	}

	// Find containers matching the service name
	// Try multiple patterns: exact name, name prefix, compose service label
	containers := c.findDockerContainers(ctx, serviceName)
	state.Containers = containers

	// Get networks
	if networks, err := c.getDockerNetworks(ctx); err == nil {
		state.Networks = networks
	}

	return state
}

// findDockerContainers finds containers matching the service name
func (c *Collector) findDockerContainers(ctx context.Context, serviceName string) []DockerContainer {
	// Get all containers with JSON output
	output, err := c.execCommand(ctx, "docker", "ps", "-a",
		"--format", "{{json .}}")
	if err != nil {
		return nil
	}

	var containers []DockerContainer
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}

		name, _ := raw["Names"].(string)
		// Match by name containing service name
		if !strings.Contains(strings.ToLower(name), strings.ToLower(serviceName)) {
			continue
		}

		container := DockerContainer{
			ID:     truncateID(raw["ID"]),
			Name:   name,
			Image:  getString(raw, "Image"),
			Status: getString(raw, "Status"),
			State:  getString(raw, "State"),
		}

		// Parse ports
		if ports := getString(raw, "Ports"); ports != "" {
			container.Ports = strings.Split(ports, ", ")
		}

		// Get detailed stats for running containers
		if container.State == "running" {
			c.enrichDockerContainer(ctx, &container)
		}

		containers = append(containers, container)
	}

	return containers
}

// enrichDockerContainer adds stats and inspect data to a container
func (c *Collector) enrichDockerContainer(ctx context.Context, container *DockerContainer) {
	// Get container inspect for more details
	output, err := c.execCommand(ctx, "docker", "inspect", container.ID)
	if err != nil {
		return
	}

	var inspectData []map[string]interface{}
	if err := json.Unmarshal([]byte(output), &inspectData); err != nil || len(inspectData) == 0 {
		return
	}

	inspect := inspectData[0]

	// Get restart count
	if state, ok := inspect["State"].(map[string]interface{}); ok {
		if rc, ok := state["RestartCount"].(float64); ok {
			container.RestartCount = int(rc)
		}
		if health, ok := state["Health"].(map[string]interface{}); ok {
			container.Health, _ = health["Status"].(string)
		}
		if startedAt, ok := state["StartedAt"].(string); ok {
			container.Uptime = startedAt // Will be parsed client-side
		}
	}

	// Get resource limits from HostConfig
	if hostConfig, ok := inspect["HostConfig"].(map[string]interface{}); ok {
		if memLimit, ok := hostConfig["Memory"].(float64); ok && memLimit > 0 {
			container.MemoryLimit = formatBytes(int64(memLimit))
		}
	}

	// Get environment variables (redacted)
	if config, ok := inspect["Config"].(map[string]interface{}); ok {
		if envList, ok := config["Env"].([]interface{}); ok {
			container.EnvVars = make(map[string]string)
			for _, e := range envList {
				if envStr, ok := e.(string); ok {
					parts := strings.SplitN(envStr, "=", 2)
					if len(parts) == 2 {
						container.EnvVars[parts[0]] = redactEnvValue(parts[0], parts[1])
					}
				}
			}
		}
	}

	// Get stats (CPU/memory usage)
	c.getDockerStats(ctx, container)
}

// getDockerStats gets CPU and memory stats for a container
func (c *Collector) getDockerStats(ctx context.Context, container *DockerContainer) {
	output, err := c.execCommand(ctx, "docker", "stats", container.ID,
		"--no-stream", "--format", "{{json .}}")
	if err != nil {
		return
	}

	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(output), &stats); err != nil {
		return
	}

	// CPU percentage
	if cpuStr := getString(stats, "CPUPerc"); cpuStr != "" {
		cpuStr = strings.TrimSuffix(cpuStr, "%")
		if cpu, err := parseFloat(cpuStr); err == nil {
			container.CPUPercent = cpu
		}
	}

	// Memory usage and percentage
	if memUsage := getString(stats, "MemUsage"); memUsage != "" {
		parts := strings.Split(memUsage, " / ")
		if len(parts) >= 1 {
			container.MemoryUsage = parts[0]
		}
		if len(parts) >= 2 {
			container.MemoryLimit = parts[1]
		}
	}

	if memPercStr := getString(stats, "MemPerc"); memPercStr != "" {
		memPercStr = strings.TrimSuffix(memPercStr, "%")
		if memPerc, err := parseFloat(memPercStr); err == nil {
			container.MemoryPercent = memPerc
		}
	}
}

// getDockerNetworks lists available networks
func (c *Collector) getDockerNetworks(ctx context.Context) ([]string, error) {
	output, err := c.execCommand(ctx, "docker", "network", "ls",
		"--format", "{{.Name}}")
	if err != nil {
		return nil, err
	}

	networks := strings.Split(output, "\n")
	var result []string
	for _, n := range networks {
		n = strings.TrimSpace(n)
		if n != "" && n != "bridge" && n != "host" && n != "none" {
			result = append(result, n)
		}
	}
	return result, nil
}

// Helper functions

func truncateID(v interface{}) string {
	s, _ := v.(string)
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1fGi", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1fMi", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1fKi", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}
