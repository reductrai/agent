# ReductrAI Agent

**Open source observability agent. Your data never leaves. Verifiable privacy.**

[![License](https://img.shields.io/badge/License-SSPL-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8.svg)](https://golang.org/)

---

## Why Open Source?

Every other observability tool asks you to **trust them** with your data. We ask you to **verify us**.

- **Banks and hospitals** need to audit code that processes sensitive logs
- **Security teams** reject obfuscated binaries they can't read
- **Compliance officers** want proof, not promises

**Our approach:** We open-source the agent that touches your data. Read every line. Verify the compression. Confirm nothing leaks.

```
"We process 100GB of logs to 2KB of math.
 Here's the code that does the shrinking."
```

---

## Quick Start

```bash
# Install
curl -sSL https://reductrai.com/install | sh

# Run in local-only mode (FREE, no license required)
reductrai start

# Or with cloud features (requires license)
reductrai start --license YOUR_KEY
```

Point your telemetry to `localhost:8080`. Auto-detected format.

---

## What's Included

| Feature | Description | License Required |
|---------|-------------|------------------|
| **Telemetry Ingestion** | OTLP, Prometheus, Datadog, and 35+ formats | No (FREE) |
| **Local Storage** | DuckDB - query your data with SQL | No (FREE) |
| **Anomaly Detection** | Error spikes, latency issues, traffic drops | No (FREE) |
| **Compression Engine** | 91-95% reduction on telemetry data | No (FREE) |
| **Bring Your Own LLM** | Use Ollama, vLLM, or any OpenAI-compatible endpoint | No (FREE) |
| **Cloud Dashboard** | Visual UI at app.reductrai.com | Yes |
| **AI Relay** | Managed LLM with optimized prompts | Yes |

---

## Architecture

```
YOUR INFRASTRUCTURE                        CLOUD (optional)
─────────────────────────────────          ────────────────────
┌────────────────────────────────┐         ┌──────────────────┐
│     ReductrAI Agent (this)     │         │  Cloud Dashboard │
│                                │         │                  │
│  Telemetry In → Compress →     │  ~2KB   │  LLM reasoning   │
│  Store (DuckDB) → Detect       │───────► │  Human approval  │
│                                │summaries│  Remediation UI  │
│  100% of data stays HERE       │         │                  │
│  Code is auditable             │         │  (optional)      │
└────────────────────────────────┘         └──────────────────┘
```

**What stays local:**
- 100% of raw logs, traces, metrics
- All PII and secrets
- Full query access (DuckDB)

**What leaves (~2KB, optional):**
- Pre-aggregated summaries only
- `"payment-service: 4.2% errors, P99 523ms"`
- No raw data. No PII. Just statistics.

---

## Local-Only Mode

Run completely air-gapped with your own LLM:

```bash
# Use Ollama
export LLM_ENDPOINT=http://localhost:11434/v1
export LLM_MODEL=llama3.2
reductrai start --local

# Use vLLM
export LLM_ENDPOINT=http://gpu-server:8000/v1
reductrai start --local
```

**What you get:**
- Full telemetry ingestion and storage
- Anomaly detection
- Local LLM investigation
- No data leaves your network. Ever.

---

## CLI Commands

```bash
reductrai start           # Start the agent
reductrai stop            # Stop the agent
reductrai status          # Check status
reductrai query "SQL"     # Run local DuckDB query
reductrai schema          # Show database schema
reductrai version         # Print version
```

### Example Queries

```bash
# Error rate by service
reductrai query "SELECT service, COUNT(*) as errors FROM spans WHERE status='ERROR' GROUP BY service"

# Top 10 slowest operations
reductrai query "SELECT service, operation, AVG(duration_us)/1000 as avg_ms FROM spans GROUP BY service, operation ORDER BY avg_ms DESC LIMIT 10"

# Recent error logs
reductrai query "SELECT timestamp, service, message FROM logs WHERE level='ERROR' ORDER BY timestamp DESC LIMIT 20"
```

---

## Building from Source

```bash
# Clone
git clone https://github.com/reductrai/agent.git
cd agent

# Build
make build

# Run
./dist/reductrai start
```

### Build Targets

```bash
make build        # Build for current platform
make build-all    # Cross-compile all platforms
make release      # Build release binaries
make test         # Run tests
make clean        # Clean build artifacts
```

---

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `REDUCTRAI_LICENSE` | - | License key (optional for local mode) |
| `REDUCTRAI_PORT` | 8080 | Telemetry ingestion port |
| `REDUCTRAI_DATA_DIR` | ~/.reductrai | DuckDB storage location |
| `LLM_ENDPOINT` | - | OpenAI-compatible LLM endpoint |
| `LLM_MODEL` | - | LLM model name |

---

## Supported Formats

**Logs:** OTLP, Datadog, Loki, FluentD, Syslog, Splunk, CloudWatch, and more
**Metrics:** Prometheus, Datadog, OTLP, StatsD, InfluxDB, Graphite, and more
**Traces:** OTLP, Jaeger, Zipkin, Datadog APM, Tempo, X-Ray, and more

Point any telemetry to `localhost:8080`. Auto-detected.

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### What We Accept

- Bug fixes
- Performance improvements
- New telemetry format support
- Documentation improvements

---

## License

**SSPL (Server Side Public License)** - See [LICENSE](LICENSE)

You can:
- Use this for any purpose
- Modify and distribute
- Build commercial products

If you offer this as a service, you must open-source your entire service stack.

---

## Support

- **Docs:** [reductrai.com/docs](https://reductrai.com/docs)
- **Issues:** [github.com/reductrai/agent/issues](https://github.com/reductrai/agent/issues)
- **Email:** support@reductrai.com

---

<div align="center">

**Your data stays local. Code is auditable. AI comes to you.**

[Get Started](https://reductrai.com) | [Cloud Dashboard](https://app.reductrai.com)

</div>
