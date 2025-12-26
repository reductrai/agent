package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/reductrai/agent/internal/context"
	"github.com/reductrai/agent/pkg/types"
)

// Client handles communication with ReductrAI cloud
type Client struct {
	baseURL    string
	licenseKey string
	httpClient *http.Client
	wsConn     *websocket.Conn
	wsMu       sync.Mutex
}

// Summary is the ~2KB payload sent to cloud
type Summary struct {
	CollectedAt      int64                 `json:"collectedAt"`
	Services         []types.ServiceHealth `json:"services"`
	TotalServices    int                   `json:"totalServices"`
	HealthyServices  int                   `json:"healthyServices"`
	DegradedServices int                   `json:"degradedServices"`
	Anomalies        []types.Anomaly       `json:"anomalies,omitempty"`
}

// RemediationCommand is received from cloud after human approval
type RemediationCommand struct {
	ID       string `json:"id"`
	Type     string `json:"type"`     // scale, restart, circuit_breaker
	Target   string `json:"target"`   // service name
	Title    string `json:"title"`
	Replicas int    `json:"replicas,omitempty"`
	Command  string `json:"command,omitempty"`
}

// NewClient creates a new cloud client
func NewClient(baseURL, licenseKey string) *Client {
	return &Client{
		baseURL:    baseURL,
		licenseKey: licenseKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ValidateLicense validates the license key with the cloud
func (c *Client) ValidateLicense() error {
	// The cloud API validates license on any authenticated request
	req, err := http.NewRequest("GET", c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-License-Key", c.licenseKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to cloud: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return fmt.Errorf("invalid license key")
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("cloud returned status %d", resp.StatusCode)
	}

	return nil
}

// SendSummary sends a telemetry summary to the cloud (~2KB)
func (c *Client) SendSummary(summary *Summary) error {
	data, err := json.Marshal(summary)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.baseURL+"/api/summaries", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-License-Key", c.licenseKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("cloud returned status %d", resp.StatusCode)
	}

	return nil
}

// ListenForCommands connects via WebSocket and listens for remediation commands
func (c *Client) ListenForCommands(ctx context.Context, handler func(RemediationCommand)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := c.connectAndListen(ctx, handler); err != nil {
				fmt.Printf("[Cloud] WebSocket error: %v, reconnecting in 5s...\n", err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (c *Client) connectAndListen(ctx context.Context, handler func(RemediationCommand)) error {
	// Build WebSocket URL
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return err
	}

	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}

	wsURL := fmt.Sprintf("%s://%s/ws?license=%s", scheme, u.Host, c.licenseKey)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.wsMu.Lock()
	c.wsConn = conn
	c.wsMu.Unlock()

	defer func() {
		c.wsMu.Lock()
		c.wsConn.Close()
		c.wsConn = nil
		c.wsMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				return err
			}

			var msg struct {
				Type          string             `json:"type"`
				Command       RemediationCommand `json:"command,omitempty"`
				InvestigationID string           `json:"investigationId,omitempty"`
				Actions       []RemediationCommand `json:"actions,omitempty"`
			}

			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "connected":
				fmt.Println("[Cloud] Connected to cloud")
			case "execute_remediation":
				fmt.Printf("[Cloud] Received remediation command for investigation %s\n", msg.InvestigationID)
				for _, action := range msg.Actions {
					handler(action)
				}
			}
		}
	}
}

// ReportCommandResult reports the result of a remediation command back to cloud
func (c *Client) ReportCommandResult(commandID, status, error string) error {
	c.wsMu.Lock()
	conn := c.wsConn
	c.wsMu.Unlock()

	if conn == nil {
		return fmt.Errorf("not connected to cloud")
	}

	msg := map[string]string{
		"type":      "command_result",
		"commandId": commandID,
		"status":    status,
		"error":     error,
	}

	data, _ := json.Marshal(msg)
	return conn.WriteMessage(websocket.TextMessage, data)
}

// EnrichedContextPayload is sent to cloud for LLM-based command generation
type EnrichedContextPayload struct {
	LicenseKey      string                    `json:"licenseKey"`
	EnrichedContext *ctxpkg.EnrichedContext   `json:"enrichedContext"`
}

// SendEnrichedContext sends enriched anomaly context to cloud for LLM analysis
// This enables data-driven remediation with custom command generation
// Returns the investigation ID if successful
func (c *Client) SendEnrichedContext(enrichedCtx *ctxpkg.EnrichedContext) (string, error) {
	payload := EnrichedContextPayload{
		LicenseKey:      c.licenseKey,
		EnrichedContext: enrichedCtx,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal context: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/api/enriched-context", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-License-Key", c.licenseKey)
	req.Header.Set("X-Binary-Version", ctxpkg.Version)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send context: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		// Old cloud version doesn't support enriched context, fall back silently
		return "", nil
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return "", fmt.Errorf("cloud returned status %d", resp.StatusCode)
	}

	// Parse response to get investigation ID
	var result struct {
		Success           bool   `json:"success"`
		InvestigationID   string `json:"investigationId"`
		CommandsGenerated int    `json:"commandsGenerated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", nil // Non-fatal, just can't get investigation ID
	}

	return result.InvestigationID, nil
}
