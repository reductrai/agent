package infra

import (
	"context"
	"strconv"
	"strings"
)

// collectSystemd gathers systemd service state
func (c *Collector) collectSystemd(ctx context.Context, serviceName string) *SystemdState {
	if !c.hasSystemctl {
		return nil
	}

	state := &SystemdState{
		Available: true,
	}

	// Try to find the service (with and without .service suffix)
	serviceNames := []string{
		serviceName,
		serviceName + ".service",
	}

	for _, svcName := range serviceNames {
		svc := c.getSystemdService(ctx, svcName)
		if svc != nil {
			state.Services = append(state.Services, *svc)
		}
	}

	return state
}

// getSystemdService retrieves information about a systemd service
func (c *Collector) getSystemdService(ctx context.Context, serviceName string) *SystemdService {
	// Get service properties using systemctl show
	output, err := c.execCommand(ctx, "systemctl", "show", serviceName,
		"--property=LoadState,ActiveState,SubState,Description,MainPID,MemoryCurrent,CPUUsageNSec,NRestarts,ActiveEnterTimestamp")
	if err != nil {
		return nil
	}

	props := parseSystemdProperties(output)

	// Skip if not loaded
	if props["LoadState"] == "not-found" {
		return nil
	}

	svc := &SystemdService{
		Name:        serviceName,
		LoadState:   props["LoadState"],
		ActiveState: props["ActiveState"],
		SubState:    props["SubState"],
		Description: props["Description"],
	}

	// Parse MainPID
	if pid, err := strconv.Atoi(props["MainPID"]); err == nil {
		svc.MainPID = pid
	}

	// Parse memory (in bytes)
	if memStr := props["MemoryCurrent"]; memStr != "" && memStr != "[not set]" {
		if mem, err := strconv.ParseInt(memStr, 10, 64); err == nil && mem > 0 {
			svc.Memory = formatBytes(mem)
		}
	}

	// Parse CPU usage (in nanoseconds)
	if cpuStr := props["CPUUsageNSec"]; cpuStr != "" && cpuStr != "[not set]" {
		if cpuNs, err := strconv.ParseInt(cpuStr, 10, 64); err == nil && cpuNs > 0 {
			// Convert to more readable format
			cpuSec := float64(cpuNs) / 1e9
			if cpuSec >= 3600 {
				svc.CPU = formatFloat(cpuSec/3600) + "h"
			} else if cpuSec >= 60 {
				svc.CPU = formatFloat(cpuSec/60) + "m"
			} else {
				svc.CPU = formatFloat(cpuSec) + "s"
			}
		}
	}

	// Parse restart count
	if restarts := props["NRestarts"]; restarts != "" {
		if r, err := strconv.Atoi(restarts); err == nil {
			svc.Restarts = r
		}
	}

	// Parse active since timestamp
	if since := props["ActiveEnterTimestamp"]; since != "" && since != "n/a" {
		svc.Since = since
	}

	return svc
}

// parseSystemdProperties parses systemctl show output
func parseSystemdProperties(output string) map[string]string {
	props := make(map[string]string)
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			props[parts[0]] = parts[1]
		}
	}

	return props
}

func formatFloat(f float64) string {
	if f == float64(int(f)) {
		return strconv.Itoa(int(f))
	}
	return strconv.FormatFloat(f, 'f', 1, 64)
}
