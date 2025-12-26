package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// collectKubernetes gathers Kubernetes state for a service
func (c *Collector) collectKubernetes(ctx context.Context, serviceName string, namespace string) *KubernetesState {
	if !c.hasKubectl {
		return nil
	}

	if namespace == "" {
		namespace = "default"
	}

	state := &KubernetesState{
		Available: true,
		Namespace: namespace,
	}

	// Get current context
	if context, err := c.execCommand(ctx, "kubectl", "config", "current-context"); err == nil {
		state.Context = context
	}

	// Try to find deployment (could be deployment, statefulset, daemonset)
	state.Deployment = c.getK8sDeployment(ctx, serviceName, namespace)

	// Get pods for this service
	state.Pods = c.getK8sPods(ctx, serviceName, namespace)

	// Get HPA if exists
	state.HPA = c.getK8sHPA(ctx, serviceName, namespace)

	// Get service
	state.Service = c.getK8sService(ctx, serviceName, namespace)

	// Get recent events
	state.RecentEvents = c.getK8sEvents(ctx, serviceName, namespace)

	return state
}

// getK8sDeployment retrieves deployment information
func (c *Collector) getK8sDeployment(ctx context.Context, serviceName string, namespace string) *K8sDeployment {
	// Try deployment first, then statefulset
	output, err := c.execCommand(ctx, "kubectl", "get", "deployment", serviceName,
		"-n", namespace, "-o", "json")
	if err != nil {
		// Try statefulset
		output, err = c.execCommand(ctx, "kubectl", "get", "statefulset", serviceName,
			"-n", namespace, "-o", "json")
		if err != nil {
			return nil
		}
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil
	}

	dep := &K8sDeployment{
		Name: serviceName,
	}

	// Parse spec
	if spec, ok := raw["spec"].(map[string]interface{}); ok {
		if replicas, ok := spec["replicas"].(float64); ok {
			dep.Replicas = int32(replicas)
		}

		// Strategy
		if strategy, ok := spec["strategy"].(map[string]interface{}); ok {
			if t, ok := strategy["type"].(string); ok {
				dep.Strategy = t
			}
			if rolling, ok := strategy["rollingUpdate"].(map[string]interface{}); ok {
				if ms, ok := rolling["maxSurge"]; ok {
					dep.MaxSurge = fmt.Sprintf("%v", ms)
				}
				if mu, ok := rolling["maxUnavailable"]; ok {
					dep.MaxUnavailable = fmt.Sprintf("%v", mu)
				}
			}
		}

		// Container spec
		if template, ok := spec["template"].(map[string]interface{}); ok {
			if podSpec, ok := template["spec"].(map[string]interface{}); ok {
				if containers, ok := podSpec["containers"].([]interface{}); ok && len(containers) > 0 {
					if container, ok := containers[0].(map[string]interface{}); ok {
						// Image
						if image, ok := container["image"].(string); ok {
							dep.Image = image
						}
						if policy, ok := container["imagePullPolicy"].(string); ok {
							dep.ImagePullPolicy = policy
						}

						// Resources
						if resources, ok := container["resources"].(map[string]interface{}); ok {
							if requests, ok := resources["requests"].(map[string]interface{}); ok {
								if cpu, ok := requests["cpu"].(string); ok {
									dep.CPURequest = cpu
								}
								if mem, ok := requests["memory"].(string); ok {
									dep.MemoryRequest = mem
								}
							}
							if limits, ok := resources["limits"].(map[string]interface{}); ok {
								if cpu, ok := limits["cpu"].(string); ok {
									dep.CPULimit = cpu
								}
								if mem, ok := limits["memory"].(string); ok {
									dep.MemoryLimit = mem
								}
							}
						}

						// Environment variables (redacted)
						if envVars, ok := container["env"].([]interface{}); ok {
							dep.EnvVars = make(map[string]string)
							for _, e := range envVars {
								if env, ok := e.(map[string]interface{}); ok {
									name, _ := env["name"].(string)
									value, _ := env["value"].(string)
									if name != "" {
										dep.EnvVars[name] = redactEnvValue(name, value)
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Parse status
	if status, ok := raw["status"].(map[string]interface{}); ok {
		if r, ok := status["readyReplicas"].(float64); ok {
			dep.ReadyReplicas = int32(r)
		}
		if a, ok := status["availableReplicas"].(float64); ok {
			dep.AvailableReplicas = int32(a)
		}
		if u, ok := status["updatedReplicas"].(float64); ok {
			dep.UpdatedReplicas = int32(u)
		}
	}

	// Parse metadata for last update time
	if metadata, ok := raw["metadata"].(map[string]interface{}); ok {
		if ts, ok := metadata["creationTimestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				dep.LastUpdated = t
			}
		}
	}

	return dep
}

// getK8sPods retrieves pod information for a service
func (c *Collector) getK8sPods(ctx context.Context, serviceName string, namespace string) []K8sPod {
	// Get pods by label selector (common patterns)
	selectors := []string{
		fmt.Sprintf("app=%s", serviceName),
		fmt.Sprintf("app.kubernetes.io/name=%s", serviceName),
	}

	var pods []K8sPod

	for _, selector := range selectors {
		output, err := c.execCommand(ctx, "kubectl", "get", "pods",
			"-n", namespace, "-l", selector, "-o", "json")
		if err != nil {
			continue
		}

		var podList struct {
			Items []map[string]interface{} `json:"items"`
		}
		if err := json.Unmarshal([]byte(output), &podList); err != nil {
			continue
		}

		if len(podList.Items) == 0 {
			continue
		}

		for _, item := range podList.Items {
			pod := K8sPod{}

			// Metadata
			if metadata, ok := item["metadata"].(map[string]interface{}); ok {
				pod.Name, _ = metadata["name"].(string)

				// Calculate age
				if ts, ok := metadata["creationTimestamp"].(string); ok {
					if t, err := time.Parse(time.RFC3339, ts); err == nil {
						pod.Age = formatDuration(time.Since(t))
					}
				}
			}

			// Spec
			if spec, ok := item["spec"].(map[string]interface{}); ok {
				pod.Node, _ = spec["nodeName"].(string)
			}

			// Status
			if status, ok := item["status"].(map[string]interface{}); ok {
				pod.Phase, _ = status["phase"].(string)

				// Container statuses
				if containerStatuses, ok := status["containerStatuses"].([]interface{}); ok {
					for _, cs := range containerStatuses {
						if containerStatus, ok := cs.(map[string]interface{}); ok {
							container := K8sContainer{}
							container.Name, _ = containerStatus["name"].(string)
							container.Ready, _ = containerStatus["ready"].(bool)

							if rc, ok := containerStatus["restartCount"].(float64); ok {
								container.RestartCount = int32(rc)
								pod.RestartCount += container.RestartCount
							}

							// Current state
							if state, ok := containerStatus["state"].(map[string]interface{}); ok {
								for stateName, stateValue := range state {
									container.State = stateName
									if sv, ok := stateValue.(map[string]interface{}); ok {
										container.Reason, _ = sv["reason"].(string)
									}
									break
								}
							}

							// Last termination reason
							if lastState, ok := containerStatus["lastState"].(map[string]interface{}); ok {
								if terminated, ok := lastState["terminated"].(map[string]interface{}); ok {
									container.LastTerminationReason, _ = terminated["reason"].(string)
								}
							}

							pod.Containers = append(pod.Containers, container)
						}
					}
				}

				// Conditions for ready check
				if conditions, ok := status["conditions"].([]interface{}); ok {
					for _, cond := range conditions {
						if c, ok := cond.(map[string]interface{}); ok {
							if t, ok := c["type"].(string); ok && t == "Ready" {
								if s, ok := c["status"].(string); ok {
									pod.Ready = s == "True"
								}
							}
						}
					}
				}
			}

			pods = append(pods, pod)
		}

		// Found pods, stop searching
		if len(pods) > 0 {
			break
		}
	}

	return pods
}

// getK8sHPA retrieves HorizontalPodAutoscaler information
func (c *Collector) getK8sHPA(ctx context.Context, serviceName string, namespace string) *K8sHPA {
	output, err := c.execCommand(ctx, "kubectl", "get", "hpa", serviceName,
		"-n", namespace, "-o", "json")
	if err != nil {
		return nil
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil
	}

	hpa := &K8sHPA{
		Name: serviceName,
	}

	// Spec
	if spec, ok := raw["spec"].(map[string]interface{}); ok {
		if min, ok := spec["minReplicas"].(float64); ok {
			hpa.MinReplicas = int32(min)
		}
		if max, ok := spec["maxReplicas"].(float64); ok {
			hpa.MaxReplicas = int32(max)
		}

		// Target metrics
		if metrics, ok := spec["metrics"].([]interface{}); ok {
			for _, m := range metrics {
				if metric, ok := m.(map[string]interface{}); ok {
					if resource, ok := metric["resource"].(map[string]interface{}); ok {
						name, _ := resource["name"].(string)
						if target, ok := resource["target"].(map[string]interface{}); ok {
							if avg, ok := target["averageUtilization"].(float64); ok {
								switch name {
								case "cpu":
									hpa.TargetCPU = int32(avg)
								case "memory":
									hpa.TargetMemory = int32(avg)
								}
							}
						}
					}
				}
			}
		}
	}

	// Status
	if status, ok := raw["status"].(map[string]interface{}); ok {
		if curr, ok := status["currentReplicas"].(float64); ok {
			hpa.CurrentReplicas = int32(curr)
		}
		if desired, ok := status["desiredReplicas"].(float64); ok {
			hpa.DesiredReplicas = int32(desired)
		}

		// Current metrics
		if currentMetrics, ok := status["currentMetrics"].([]interface{}); ok {
			for _, m := range currentMetrics {
				if metric, ok := m.(map[string]interface{}); ok {
					if resource, ok := metric["resource"].(map[string]interface{}); ok {
						name, _ := resource["name"].(string)
						if current, ok := resource["current"].(map[string]interface{}); ok {
							if avg, ok := current["averageUtilization"].(float64); ok {
								switch name {
								case "cpu":
									hpa.CurrentCPU = int32(avg)
								case "memory":
									hpa.CurrentMemory = int32(avg)
								}
							}
						}
					}
				}
			}
		}
	}

	return hpa
}

// getK8sService retrieves Service information
func (c *Collector) getK8sService(ctx context.Context, serviceName string, namespace string) *K8sService {
	output, err := c.execCommand(ctx, "kubectl", "get", "service", serviceName,
		"-n", namespace, "-o", "json")
	if err != nil {
		return nil
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil
	}

	svc := &K8sService{
		Name: serviceName,
	}

	if spec, ok := raw["spec"].(map[string]interface{}); ok {
		svc.Type, _ = spec["type"].(string)
		svc.ClusterIP, _ = spec["clusterIP"].(string)

		if ports, ok := spec["ports"].([]interface{}); ok {
			for _, p := range ports {
				if port, ok := p.(map[string]interface{}); ok {
					portNum, _ := port["port"].(float64)
					protocol, _ := port["protocol"].(string)
					name, _ := port["name"].(string)
					if name != "" {
						svc.Ports = append(svc.Ports, fmt.Sprintf("%s:%d/%s", name, int(portNum), protocol))
					} else {
						svc.Ports = append(svc.Ports, fmt.Sprintf("%d/%s", int(portNum), protocol))
					}
				}
			}
		}
	}

	return svc
}

// getK8sEvents retrieves recent events for a service
func (c *Collector) getK8sEvents(ctx context.Context, serviceName string, namespace string) []K8sEvent {
	// Get events related to the deployment/pods
	output, err := c.execCommand(ctx, "kubectl", "get", "events",
		"-n", namespace,
		"--field-selector", fmt.Sprintf("involvedObject.name=%s", serviceName),
		"--sort-by=.lastTimestamp",
		"-o", "json")
	if err != nil {
		// Try getting events for pods with this service name prefix
		output, err = c.execCommand(ctx, "kubectl", "get", "events",
			"-n", namespace,
			"-o", "json")
		if err != nil {
			return nil
		}
	}

	var eventList struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal([]byte(output), &eventList); err != nil {
		return nil
	}

	var events []K8sEvent
	cutoff := time.Now().Add(-15 * time.Minute)

	for _, item := range eventList.Items {
		// Check if event is recent and relevant
		involvedObject, _ := item["involvedObject"].(map[string]interface{})
		objName, _ := involvedObject["name"].(string)

		// Filter to events related to this service
		if !strings.HasPrefix(objName, serviceName) && objName != serviceName {
			continue
		}

		event := K8sEvent{}
		event.Type, _ = item["type"].(string)
		event.Reason, _ = item["reason"].(string)
		event.Message, _ = item["message"].(string)

		if count, ok := item["count"].(float64); ok {
			event.Count = int32(count)
		}

		if lastTs, ok := item["lastTimestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339, lastTs); err == nil {
				event.LastSeen = t
				// Skip old events
				if t.Before(cutoff) {
					continue
				}
			}
		}

		// Only include Warning events or interesting Normal events
		if event.Type == "Warning" ||
			event.Reason == "Unhealthy" ||
			event.Reason == "BackOff" ||
			event.Reason == "Failed" ||
			event.Reason == "OOMKilling" ||
			event.Reason == "Evicted" {
			events = append(events, event)
		}

		// Limit to 10 most recent
		if len(events) >= 10 {
			break
		}
	}

	return events
}

// formatDuration formats a duration to human readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	return fmt.Sprintf("%dd", days)
}

// parseQuantity parses k8s resource quantity to a value
func parseQuantity(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty quantity")
	}

	// Handle memory units
	multiplier := int64(1)
	if strings.HasSuffix(s, "Ki") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "Ki")
	} else if strings.HasSuffix(s, "Mi") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "Mi")
	} else if strings.HasSuffix(s, "Gi") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "Gi")
	} else if strings.HasSuffix(s, "m") {
		// millicores
		s = strings.TrimSuffix(s, "m")
		val, err := strconv.ParseInt(s, 10, 64)
		return val, err
	}

	val, err := strconv.ParseInt(s, 10, 64)
	return val * multiplier, err
}
