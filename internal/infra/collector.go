package infra

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// Collector gathers infrastructure state for a given service
type Collector struct {
	timeout time.Duration

	// Detected infrastructure
	hasKubectl  bool
	hasDocker   bool
	hasSystemctl bool
}

// NewCollector creates a new infrastructure state collector
func NewCollector() *Collector {
	c := &Collector{
		timeout: 10 * time.Second,
	}
	c.detectInfrastructure()
	return c
}

// detectInfrastructure checks which infrastructure tools are available
func (c *Collector) detectInfrastructure() {
	// Check for kubectl
	if _, err := exec.LookPath("kubectl"); err == nil {
		// Verify kubectl can connect
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := exec.CommandContext(ctx, "kubectl", "cluster-info").Run(); err == nil {
			c.hasKubectl = true
		}
	}

	// Check for docker
	if _, err := exec.LookPath("docker"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := exec.CommandContext(ctx, "docker", "info").Run(); err == nil {
			c.hasDocker = true
		}
	}

	// Check for systemctl
	if _, err := exec.LookPath("systemctl"); err == nil {
		c.hasSystemctl = true
	}
}

// AvailableInfra returns which infrastructure types are available
func (c *Collector) AvailableInfra() map[string]bool {
	return map[string]bool{
		"kubernetes": c.hasKubectl,
		"docker":     c.hasDocker,
		"systemd":    c.hasSystemctl,
	}
}

// Collect gathers infrastructure state for a service
func (c *Collector) Collect(ctx context.Context, serviceName string, namespace string) (*InfraState, error) {
	state := &InfraState{
		CollectedAt: time.Now(),
	}

	// Collect from all available infrastructure in parallel
	type result struct {
		k8s     *KubernetesState
		docker  *DockerState
		systemd *SystemdState
	}

	resultCh := make(chan result, 1)

	go func() {
		r := result{}

		if c.hasKubectl {
			r.k8s = c.collectKubernetes(ctx, serviceName, namespace)
		}

		if c.hasDocker {
			r.docker = c.collectDocker(ctx, serviceName)
		}

		if c.hasSystemctl {
			r.systemd = c.collectSystemd(ctx, serviceName)
		}

		resultCh <- r
	}()

	select {
	case r := <-resultCh:
		state.Kubernetes = r.k8s
		state.Docker = r.docker
		state.Systemd = r.systemd
	case <-ctx.Done():
		return state, ctx.Err()
	}

	return state, nil
}

// execCommand runs a command and returns output
func (c *Collector) execCommand(ctx context.Context, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// redactEnvValue redacts sensitive environment variable values
func redactEnvValue(key, value string) string {
	sensitiveKeys := []string{
		"password", "secret", "token", "key", "credential",
		"auth", "api_key", "apikey", "private", "cert",
	}

	keyLower := strings.ToLower(key)
	for _, sensitive := range sensitiveKeys {
		if strings.Contains(keyLower, sensitive) {
			return "[REDACTED]"
		}
	}

	// Redact if value looks like a secret
	if len(value) > 20 && !strings.Contains(value, " ") {
		// Long random-looking strings
		return "[REDACTED]"
	}

	return value
}
