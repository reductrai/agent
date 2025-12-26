package detector

import (
	"sync"
	"time"

	"github.com/reductrai/agent/internal/storage"
	"github.com/reductrai/agent/pkg/types"
)

// Baseline-relative thresholds for anomaly detection
// These multipliers detect when current values exceed learned baselines
const (
	// A value 3x the baseline is considered a spike
	ErrorRateSpikeMultiplier  = 3.0
	LatencySpikeMultiplier    = 3.0
	TrafficDropThreshold      = 0.5 // 50% drop from baseline

	// Minimum floors to avoid false positives on very low baselines
	MinErrorRateFloor  = 0.01 // Don't alert below 1% even if 3x baseline
	MinLatencyFloor    = 100  // Don't alert below 100ms even if 3x baseline
	MinBaselineSamples = 5    // Need at least 5 samples before using baseline
)

// AnomalyDetector detects anomalies from telemetry data
type AnomalyDetector struct {
	storage   *storage.DuckDB
	anomalies []types.Anomaly
	baselines map[string]*ServiceBaseline
	mu        sync.RWMutex
}

// ServiceBaseline stores learned baseline metrics for comparison
type ServiceBaseline struct {
	Service        string
	AvgErrorRate   float64
	AvgLatencyP99  float64
	AvgRequestRate float64
	SampleCount    int // Number of samples used to build baseline
	LastUpdated    time.Time
}

// New creates a new anomaly detector
func New(storage *storage.DuckDB) *AnomalyDetector {
	return &AnomalyDetector{
		storage:   storage,
		baselines: make(map[string]*ServiceBaseline),
	}
}

// Detect runs anomaly detection and returns any new anomalies
func (d *AnomalyDetector) Detect() []types.Anomaly {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear old anomalies (older than 5 minutes)
	now := time.Now()
	var activeAnomalies []types.Anomaly
	for _, a := range d.anomalies {
		if now.Sub(a.DetectedAt) < 5*time.Minute {
			activeAnomalies = append(activeAnomalies, a)
		}
	}
	d.anomalies = activeAnomalies

	// Get current service health
	services := d.storage.GetServiceHealth()

	var newAnomalies []types.Anomaly

	for _, svc := range services {
		baseline := d.getOrCreateBaseline(svc)

		// Only use baseline-relative detection after we have enough samples
		hasEnoughSamples := baseline.SampleCount >= MinBaselineSamples

		// Check error rate spike (baseline-relative)
		if hasEnoughSamples && baseline.AvgErrorRate > 0 {
			// Calculate dynamic threshold: baseline * multiplier, with floor
			errorThreshold := baseline.AvgErrorRate * ErrorRateSpikeMultiplier
			if errorThreshold < MinErrorRateFloor {
				errorThreshold = MinErrorRateFloor
			}

			if svc.ErrorRate > errorThreshold {
				severity := d.calculateSeverity(svc.ErrorRate, errorThreshold)
				if !d.hasActiveAnomaly(svc.Service, "error_spike") {
					anomaly := types.Anomaly{
						Service:    svc.Service,
						Type:       "error_spike",
						Severity:   severity,
						Value:      svc.ErrorRate,
						Threshold:  errorThreshold,
						DetectedAt: now,
					}
					d.anomalies = append(d.anomalies, anomaly)
					newAnomalies = append(newAnomalies, anomaly)
				}
			}
		}

		// Check latency spike (baseline-relative)
		if hasEnoughSamples && baseline.AvgLatencyP99 > 0 {
			// Calculate dynamic threshold: baseline * multiplier, with floor
			latencyThreshold := baseline.AvgLatencyP99 * LatencySpikeMultiplier
			if latencyThreshold < MinLatencyFloor {
				latencyThreshold = MinLatencyFloor
			}

			if svc.LatencyP99Ms > latencyThreshold {
				severity := d.calculateSeverity(svc.LatencyP99Ms, latencyThreshold)
				if !d.hasActiveAnomaly(svc.Service, "latency_spike") {
					anomaly := types.Anomaly{
						Service:    svc.Service,
						Type:       "latency_spike",
						Severity:   severity,
						Value:      svc.LatencyP99Ms,
						Threshold:  latencyThreshold,
						DetectedAt: now,
					}
					d.anomalies = append(d.anomalies, anomaly)
					newAnomalies = append(newAnomalies, anomaly)
				}
			}
		}

		// Check traffic drop (baseline-relative)
		if hasEnoughSamples && baseline.AvgRequestRate > 0 {
			dropRatio := svc.RequestsPerMin / baseline.AvgRequestRate
			if dropRatio < TrafficDropThreshold {
				if !d.hasActiveAnomaly(svc.Service, "traffic_drop") {
					anomaly := types.Anomaly{
						Service:    svc.Service,
						Type:       "traffic_drop",
						Severity:   "medium",
						Value:      dropRatio,
						Threshold:  TrafficDropThreshold,
						DetectedAt: now,
					}
					d.anomalies = append(d.anomalies, anomaly)
					newAnomalies = append(newAnomalies, anomaly)
				}
			}
		}

		// Update baseline with exponential moving average
		d.updateBaseline(svc)
	}

	return newAnomalies
}

// GetActiveAnomalies returns all currently active anomalies
func (d *AnomalyDetector) GetActiveAnomalies() []types.Anomaly {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Filter to anomalies from last 5 minutes
	now := time.Now()
	var active []types.Anomaly
	for _, a := range d.anomalies {
		if now.Sub(a.DetectedAt) < 5*time.Minute {
			active = append(active, a)
		}
	}
	return active
}

func (d *AnomalyDetector) hasActiveAnomaly(service, anomalyType string) bool {
	now := time.Now()
	for _, a := range d.anomalies {
		if a.Service == service && a.Type == anomalyType && now.Sub(a.DetectedAt) < 5*time.Minute {
			return true
		}
	}
	return false
}

func (d *AnomalyDetector) calculateSeverity(value, threshold float64) string {
	ratio := value / threshold
	if ratio > 5 {
		return "critical"
	} else if ratio > 3 {
		return "high"
	} else if ratio > 2 {
		return "medium"
	}
	return "low"
}

func (d *AnomalyDetector) getOrCreateBaseline(svc types.ServiceHealth) *ServiceBaseline {
	baseline, exists := d.baselines[svc.Service]
	if !exists {
		baseline = &ServiceBaseline{
			Service:        svc.Service,
			AvgErrorRate:   svc.ErrorRate,
			AvgLatencyP99:  svc.LatencyP99Ms,
			AvgRequestRate: svc.RequestsPerMin,
			SampleCount:    1,
			LastUpdated:    time.Now(),
		}
		d.baselines[svc.Service] = baseline
	}
	return baseline
}

func (d *AnomalyDetector) updateBaseline(svc types.ServiceHealth) {
	baseline := d.baselines[svc.Service]
	if baseline == nil {
		return
	}

	// Exponential moving average (alpha = 0.1 for smoothing)
	// Lower alpha = slower adaptation = more stable baseline
	alpha := 0.1
	baseline.AvgErrorRate = alpha*svc.ErrorRate + (1-alpha)*baseline.AvgErrorRate
	baseline.AvgLatencyP99 = alpha*svc.LatencyP99Ms + (1-alpha)*baseline.AvgLatencyP99
	baseline.AvgRequestRate = alpha*svc.RequestsPerMin + (1-alpha)*baseline.AvgRequestRate
	baseline.SampleCount++
	baseline.LastUpdated = time.Now()
}
