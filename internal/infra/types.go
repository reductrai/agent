package infra

import "time"

// InfraState represents the current state of infrastructure components
// This is sent to the LLM so it can generate specific remediation commands
type InfraState struct {
	CollectedAt  time.Time              `json:"collectedAt"`
	Kubernetes   *KubernetesState       `json:"kubernetes,omitempty"`
	Docker       *DockerState           `json:"docker,omitempty"`
	Systemd      *SystemdState          `json:"systemd,omitempty"`
	Database     *DatabaseState         `json:"database,omitempty"`
	Cache        *CacheState            `json:"cache,omitempty"`
	LoadBalancer *LoadBalancerState     `json:"loadBalancer,omitempty"`
	Custom       map[string]interface{} `json:"custom,omitempty"`
}

// KubernetesState captures k8s deployment state for a service
type KubernetesState struct {
	Available bool   `json:"available"`
	Context   string `json:"context,omitempty"`
	Namespace string `json:"namespace,omitempty"`

	// Deployment info
	Deployment   *K8sDeployment `json:"deployment,omitempty"`
	Pods         []K8sPod       `json:"pods,omitempty"`
	HPA          *K8sHPA        `json:"hpa,omitempty"`
	Service      *K8sService    `json:"service,omitempty"`
	RecentEvents []K8sEvent     `json:"recentEvents,omitempty"`
}

type K8sDeployment struct {
	Name              string `json:"name"`
	Replicas          int32  `json:"replicas"`
	ReadyReplicas     int32  `json:"readyReplicas"`
	AvailableReplicas int32  `json:"availableReplicas"`
	UpdatedReplicas   int32  `json:"updatedReplicas"`
	Strategy          string `json:"strategy"` // RollingUpdate, Recreate
	MaxSurge          string `json:"maxSurge,omitempty"`
	MaxUnavailable    string `json:"maxUnavailable,omitempty"`

	// Resource limits/requests
	CPURequest    string `json:"cpuRequest,omitempty"`
	CPULimit      string `json:"cpuLimit,omitempty"`
	MemoryRequest string `json:"memoryRequest,omitempty"`
	MemoryLimit   string `json:"memoryLimit,omitempty"`

	// Key environment variables (redacted)
	EnvVars map[string]string `json:"envVars,omitempty"`

	// Container image
	Image           string `json:"image"`
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Last update
	LastUpdated time.Time `json:"lastUpdated,omitempty"`
}

type K8sPod struct {
	Name         string `json:"name"`
	Phase        string `json:"phase"` // Running, Pending, Failed, etc.
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restartCount"`
	Age          string `json:"age"`
	Node         string `json:"node,omitempty"`

	// Resource usage (if metrics available)
	CPUUsage    string `json:"cpuUsage,omitempty"`
	MemoryUsage string `json:"memoryUsage,omitempty"`

	// Container statuses
	Containers []K8sContainer `json:"containers,omitempty"`
}

type K8sContainer struct {
	Name                  string `json:"name"`
	Ready                 bool   `json:"ready"`
	RestartCount          int32  `json:"restartCount"`
	State                 string `json:"state"`            // running, waiting, terminated
	Reason                string `json:"reason,omitempty"` // OOMKilled, CrashLoopBackOff, etc.
	LastTerminationReason string `json:"lastTerminationReason,omitempty"`
}

type K8sHPA struct {
	Name            string `json:"name"`
	MinReplicas     int32  `json:"minReplicas"`
	MaxReplicas     int32  `json:"maxReplicas"`
	CurrentReplicas int32  `json:"currentReplicas"`
	DesiredReplicas int32  `json:"desiredReplicas"`

	// Current metrics
	CurrentCPU    int32 `json:"currentCPU,omitempty"` // percentage
	TargetCPU     int32 `json:"targetCPU,omitempty"`
	CurrentMemory int32 `json:"currentMemory,omitempty"` // percentage
	TargetMemory  int32 `json:"targetMemory,omitempty"`
}

type K8sService struct {
	Name      string   `json:"name"`
	Type      string   `json:"type"` // ClusterIP, NodePort, LoadBalancer
	ClusterIP string   `json:"clusterIP,omitempty"`
	Ports     []string `json:"ports,omitempty"`
}

type K8sEvent struct {
	Type     string    `json:"type"`   // Normal, Warning
	Reason   string    `json:"reason"` // Scheduled, Pulled, Started, Unhealthy, etc.
	Message  string    `json:"message"`
	Count    int32     `json:"count"`
	LastSeen time.Time `json:"lastSeen"`
}

// DockerState captures Docker container state
type DockerState struct {
	Available  bool              `json:"available"`
	Containers []DockerContainer `json:"containers,omitempty"`
	Networks   []string          `json:"networks,omitempty"`
}

type DockerContainer struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Image        string `json:"image"`
	Status       string `json:"status"` // running, paused, exited
	State        string `json:"state"`
	Health       string `json:"health,omitempty"` // healthy, unhealthy, starting
	RestartCount int    `json:"restartCount"`
	Uptime       string `json:"uptime,omitempty"`

	// Resource usage
	CPUPercent    float64 `json:"cpuPercent,omitempty"`
	MemoryUsage   string  `json:"memoryUsage,omitempty"`
	MemoryLimit   string  `json:"memoryLimit,omitempty"`
	MemoryPercent float64 `json:"memoryPercent,omitempty"`

	// Ports
	Ports []string `json:"ports,omitempty"`

	// Environment (redacted)
	EnvVars map[string]string `json:"envVars,omitempty"`
}

// SystemdState captures systemd service state
type SystemdState struct {
	Available bool             `json:"available"`
	Services  []SystemdService `json:"services,omitempty"`
}

type SystemdService struct {
	Name        string `json:"name"`
	LoadState   string `json:"loadState"`   // loaded, not-found, masked
	ActiveState string `json:"activeState"` // active, inactive, failed
	SubState    string `json:"subState"`    // running, dead, failed, exited
	Description string `json:"description,omitempty"`
	MainPID     int    `json:"mainPID,omitempty"`
	Memory      string `json:"memory,omitempty"`
	CPU         string `json:"cpu,omitempty"`
	Restarts    int    `json:"restarts,omitempty"`
	Since       string `json:"since,omitempty"`
}

// DatabaseState captures database connection and performance state
type DatabaseState struct {
	Type      string `json:"type"` // postgres, mysql, mongodb
	Available bool   `json:"available"`

	// Connection pool
	ActiveConnections  int `json:"activeConnections,omitempty"`
	IdleConnections    int `json:"idleConnections,omitempty"`
	MaxConnections     int `json:"maxConnections,omitempty"`
	WaitingConnections int `json:"waitingConnections,omitempty"`

	// Replication (if applicable)
	ReplicationLag   string `json:"replicationLag,omitempty"`
	ReplicationState string `json:"replicationState,omitempty"`

	// Performance
	SlowQueries   int     `json:"slowQueries,omitempty"`
	DeadlockCount int     `json:"deadlockCount,omitempty"`
	CacheHitRatio float64 `json:"cacheHitRatio,omitempty"`

	// Storage
	DatabaseSize string `json:"databaseSize,omitempty"`
	TableBloat   string `json:"tableBloat,omitempty"`
}

// CacheState captures Redis/Memcached state
type CacheState struct {
	Type      string `json:"type"` // redis, memcached
	Available bool   `json:"available"`

	// Memory
	UsedMemory    string  `json:"usedMemory,omitempty"`
	MaxMemory     string  `json:"maxMemory,omitempty"`
	MemoryPercent float64 `json:"memoryPercent,omitempty"`

	// Performance
	HitRate     float64 `json:"hitRate,omitempty"`
	MissRate    float64 `json:"missRate,omitempty"`
	EvictedKeys int64   `json:"evictedKeys,omitempty"`

	// Connections
	ConnectedClients int `json:"connectedClients,omitempty"`
	BlockedClients   int `json:"blockedClients,omitempty"`

	// Replication
	Role           string `json:"role,omitempty"` // master, slave
	ReplicationLag int    `json:"replicationLag,omitempty"`
}

// LoadBalancerState captures LB/Ingress state
type LoadBalancerState struct {
	Type      string         `json:"type"` // nginx, haproxy, alb, elb
	Available bool           `json:"available"`
	Backends  []BackendState `json:"backends,omitempty"`

	// Traffic
	ActiveConnections int64 `json:"activeConnections,omitempty"`
	RequestsPerSec    int64 `json:"requestsPerSec,omitempty"`

	// Health
	HealthyBackends   int `json:"healthyBackends,omitempty"`
	UnhealthyBackends int `json:"unhealthyBackends,omitempty"`
}

type BackendState struct {
	Name              string `json:"name"`
	Address           string `json:"address,omitempty"`
	Status            string `json:"status"` // healthy, unhealthy, draining
	Weight            int    `json:"weight,omitempty"`
	ActiveConnections int    `json:"activeConnections,omitempty"`
}
