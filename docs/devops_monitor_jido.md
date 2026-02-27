# Intelligent DevOps Monitor with Jido

A multi-agent system built on the [Jido](https://github.com/agentjido/jido) autonomous agent framework that watches TimelessMetrics data (metrics, logs, traces) and uses LLM-powered analysis to produce actionable, natural language alerting.

## Why

Traditional alerting is threshold-based: "CPU > 90% for 5 minutes." This misses context. A spike in memory might be normal during a scheduled batch job but alarming during normal traffic. An intelligent monitor can correlate signals across metrics, logs, and traces to produce explanations like:

> "Memory usage spiked 40% in the last 5 minutes, correlating with a 3x increase in request rate to /api/reports. Traces show report generation queries are holding connections for 12s on average. This looks like the report endpoint is not releasing memory properly under load."

## Architecture

Four agents communicate via Jido signals:

```
TimelessMetrics data
        |
        v
+-------------------+      signals      +---------------------+
| MetricsCollector  | ----------------> | AnomalyDetector     |
| Agent             |                   | Agent               |
+-------------------+                   +---------------------+
                                                |
                                          anomaly signals
                                                |
                                                v
                                        +-------------------+
                                        | Analyst Agent     |
                                        | (LLM via jido_ai) |
                                        +-------------------+
                                                |
                                          analysis signals
                                                |
                                                v
                                        +-------------------+
                                        | Notifier Agent    |
                                        | (Slack/email/etc) |
                                        +-------------------+
```

### Agent 1: MetricsCollector

Periodically queries TimelessMetrics for current system state.

**Responsibilities:**
- Poll metrics (CPU, memory, request latency, error rates, queue depths)
- Query recent log entries for error patterns
- Sample trace data for latency distributions
- Emit normalized metric snapshots as signals

**Jido primitives used:** Agent, Actions, Schedule directive

```elixir
defmodule DevopsMonitor.MetricsCollector do
  use Jido.Agent,
    name: "metrics_collector",
    schema: [
      poll_interval_ms: [type: :integer, default: 30_000],
      metric_queries: [type: {:list, :string}, default: []]
    ]
end

defmodule DevopsMonitor.Actions.CollectMetrics do
  use Jido.Action,
    name: "collect_metrics",
    schema: [
      metric_names: [type: {:list, :string}, required: true]
    ]

  def run(params, context) do
    # Query TimelessMetrics for current values
    snapshots = Enum.map(params.metric_names, fn name ->
      {name, TimelessMetrics.query(name, last: :5m)}
    end)

    {:ok, %{snapshots: snapshots, collected_at: DateTime.utc_now()}}
  end
end
```

### Agent 2: AnomalyDetector

Receives metric snapshots and applies detection strategies.

**Responsibilities:**
- Maintain rolling windows of metric history
- Detect threshold breaches, sudden spikes/drops, sustained trends
- Correlate anomalies across multiple metric streams
- Emit anomaly signals with context (affected metrics, severity, timeframe)

**Detection strategies (start simple, add more over time):**
- Static thresholds (configurable per metric)
- Standard deviation from rolling mean
- Rate-of-change detection
- Cross-metric correlation (e.g., error rate + latency both spiking)

```elixir
defmodule DevopsMonitor.AnomalyDetector do
  use Jido.Agent,
    name: "anomaly_detector",
    schema: [
      thresholds: [type: :map, default: %{}],
      rolling_windows: [type: :map, default: %{}],
      window_size: [type: :integer, default: 20]
    ],
    signal_routes: [
      {"metrics.snapshot", DevopsMonitor.Actions.DetectAnomalies}
    ]
end
```

### Agent 3: Analyst (LLM-powered)

Receives anomaly signals and produces natural language analysis.

**Responsibilities:**
- Receive anomaly context (which metrics, what values, timeframe)
- Query additional context from TimelessMetrics (related logs, traces)
- Use LLM to synthesize a human-readable explanation
- Suggest potential root causes and remediation steps
- Rate severity based on business impact

```elixir
defmodule DevopsMonitor.Analyst do
  use Jido.Agent, name: "analyst"

  def start_link(opts \\ []) do
    Jido.AI.Agent.start_link(
      agent: __MODULE__,
      ai: [
        model: {:anthropic, model: "claude-sonnet-4-6"},
        prompt: analyst_prompt(),
        tools: [
          DevopsMonitor.Tools.QueryMetrics,
          DevopsMonitor.Tools.QueryLogs,
          DevopsMonitor.Tools.QueryTraces
        ]
      ]
    )
  end

  defp analyst_prompt do
    Jido.AI.Prompt.new(%{
      messages: [
        %{
          role: :system,
          content: """
          You are a senior SRE analyzing system anomalies. You have access to
          metrics, logs, and traces. When given an anomaly:
          1. Investigate correlated signals across metrics, logs, and traces
          2. Identify the most likely root cause
          3. Assess severity (critical/warning/info)
          4. Suggest immediate remediation steps
          Be concise and actionable. Lead with the most important finding.
          """,
          engine: :eex
        },
        %{
          role: :user,
          content: "<anomaly><%= @anomaly_context %></anomaly>",
          engine: :eex
        }
      ]
    })
  end
end
```

### Agent 4: Notifier

Dispatches formatted alerts to configured channels.

**Responsibilities:**
- Receive analysis signals from the Analyst
- Format for each output channel (Slack, email, PagerDuty, webhook)
- Deduplicate / rate-limit to avoid alert fatigue
- Track acknowledgment state

```elixir
defmodule DevopsMonitor.Notifier do
  use Jido.Agent,
    name: "notifier",
    schema: [
      channels: [type: {:list, :atom}, default: [:slack]],
      rate_limit_seconds: [type: :integer, default: 300],
      recent_alerts: [type: :map, default: %{}]
    ],
    signal_routes: [
      {"analysis.complete", DevopsMonitor.Actions.Dispatch}
    ]
end
```

## Dependencies

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:jido_ai, "~> 0.5"},
    {:jido_signal, "~> 0.3"},
    {:req_llm, "~> 0.3"},
    {:timeless_metrics, path: "../timeless_metrics"}
  ]
end
```

## Prerequisites

Before building this, we need sufficient operational data flowing through TimelessMetrics:

- [ ] Consistent metric collection across services (CPU, memory, request rates, error rates)
- [ ] Structured log ingestion with correlation IDs
- [ ] Distributed tracing with span data queryable by time range
- [ ] Enough historical data to establish baselines (2+ weeks of normal operation)
- [ ] Query APIs in TimelessMetrics that support time-range and aggregation queries

## Implementation Phases

### Phase 1: Single-agent metrics polling
- MetricsCollector agent with basic TimelessMetrics queries
- Static threshold detection inline (no separate detector agent yet)
- Console output only (no LLM, no notifications)
- Goal: validate Jido agent lifecycle and TimelessMetrics integration

### Phase 2: Multi-agent anomaly detection
- Split out AnomalyDetector as a separate agent
- Add signal-based communication between collector and detector
- Implement rolling window and rate-of-change detection
- Goal: reliable anomaly detection without LLM dependency

### Phase 3: LLM-powered analysis
- Add Analyst agent with jido_ai
- Implement tools for querying metrics/logs/traces from the LLM
- Cross-signal correlation (metrics + logs + traces in one analysis)
- Goal: natural language explanations of anomalies

### Phase 4: Notification and production hardening
- Add Notifier agent with Slack/email/webhook support
- Rate limiting, deduplication, acknowledgment tracking
- Persistence (hibernate/thaw) for agent state across restarts
- Dashboard via LiveView showing agent status and recent analyses

## References

- [Jido Documentation](https://hexdocs.pm/jido/readme.html)
- [Jido AI Documentation](https://hexdocs.pm/jido_ai/getting-started.html)
- [Jido GitHub](https://github.com/agentjido/jido)
- [Weather Agent Tutorial](https://agentjido.xyz/blog/weather-agent) (good starter example)
- [Appunite: Integrating AI with Jido](https://www.appunite.com/blog/integrating-generative-ai-into-elixir-based-applications-by-using-the-jido-agentic-framework)
