# Alerting

TimelessMetrics has a built-in alert engine that evaluates threshold rules against live metric data and delivers webhook notifications on state changes.

## How It Works

Alert evaluation piggybacks on the **rollup tick** — no extra timers or polling. Every rollup interval (default: 5 minutes), TimelessMetrics:

1. Queries the latest aggregated value for each rule's metric + labels
2. Checks if the value breaches the rule's threshold condition
3. Transitions the alert state machine
4. Fires a webhook on `firing` and `resolved` transitions

## State Machine

Each alert rule tracks state **per series** (unique label combination):

```
ok → pending → firing → resolved → ok
```

| State | Meaning |
|---|---|
| `ok` | Value is within normal range |
| `pending` | Value is breaching, waiting for `duration` to elapse |
| `firing` | Breach lasted longer than `duration` — webhook sent |
| `resolved` | Was firing, now back to normal — webhook sent |

If `duration` is 0, the alert skips `pending` and fires immediately on breach.

When an alert returns to `ok` from `resolved`, its state row is automatically deleted from the database. This keeps the `alert_state` table self-regulating — only active states (`pending`, `firing`, `resolved`) persist. In high-cardinality environments (thousands of devices), this prevents unbounded growth of state rows for series that are operating normally.

Alert **rules** persist until explicitly deleted. If you decommission devices or rename metrics, delete the corresponding rules to avoid orphaned evaluations.

## Elixir API

### Create a rule

```elixir
{:ok, rule_id} =
  TimelessMetrics.create_alert(:metrics,
    name: "High CPU",
    metric: "cpu_usage",
    labels: %{"host" => "web-1"},
    condition: :above,
    threshold: 90.0,
    duration: 300,          # must breach for 5 minutes before firing
    aggregate: :avg,        # aggregate function for the lookback window
    webhook_url: "http://localhost:4000/hooks/alerts"
  )
```

### Required options

| Option | Type | Description |
|---|---|---|
| `:name` | string | Human-readable alert name |
| `:metric` | string | Metric name to monitor |
| `:condition` | `:above` or `:below` | Threshold direction |
| `:threshold` | number | Numeric threshold value |

### Optional

| Option | Type | Default | Description |
|---|---|---|---|
| `:labels` | map | `%{}` | Label filter (empty = all series for this metric) |
| `:duration` | integer | `0` | Seconds the value must breach before firing |
| `:aggregate` | atom | `:avg` | Aggregate function (`:avg`, `:min`, `:max`, `:sum`, `:last`) |
| `:webhook_url` | string | `nil` | URL for webhook notifications |

### List rules

```elixir
{:ok, rules} = TimelessMetrics.list_alerts(:metrics)

# Each rule includes current state per series:
# %{
#   id: 1,
#   name: "High CPU",
#   metric: "cpu_usage",
#   labels: %{"host" => "web-1"},
#   condition: "above",
#   threshold: 90.0,
#   duration: 300,
#   aggregate: "avg",
#   webhook_url: "http://...",
#   enabled: true,
#   states: [
#     %{series_labels: %{"host" => "web-1"}, state: "firing",
#       triggered_at: 1707350400, last_value: 94.2}
#   ]
# }
```

### Delete a rule

```elixir
:ok = TimelessMetrics.delete_alert(:metrics, rule_id)
```

### Manual evaluation

Alert rules are evaluated automatically on every rollup tick. To trigger evaluation manually:

```elixir
TimelessMetrics.evaluate_alerts(:metrics)
```

## HTTP API

### Create a rule

```bash
curl -X POST http://localhost:8428/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "High CPU",
    "metric": "cpu_usage",
    "labels": {"host": "web-1"},
    "condition": "above",
    "threshold": 90.0,
    "duration": 300,
    "aggregate": "avg",
    "webhook_url": "http://localhost:4000/hooks/alerts"
  }'
```

Response:

```json
{"id": 1, "status": "created"}
```

### List all rules

```bash
curl http://localhost:8428/api/v1/alerts
```

Response:

```json
{
  "data": [
    {
      "id": 1,
      "name": "High CPU",
      "metric": "cpu_usage",
      "labels": {"host": "web-1"},
      "condition": "above",
      "threshold": 90.0,
      "duration": 300,
      "aggregate": "avg",
      "webhook_url": "http://localhost:4000/hooks/alerts",
      "enabled": true,
      "states": [
        {
          "series_labels": {"host": "web-1"},
          "state": "ok",
          "triggered_at": null,
          "last_value": 42.3
        }
      ]
    }
  ]
}
```

### Delete a rule

```bash
curl -X DELETE http://localhost:8428/api/v1/alerts/1
```

## Webhook Payload

When an alert transitions to `firing` or `resolved`, TimelessMetrics POSTs JSON to the configured `webhook_url`:

```json
{
  "alert": "High CPU",
  "metric": "cpu_usage",
  "labels": {"host": "web-1"},
  "value": 94.2,
  "threshold": 90.0,
  "condition": "above",
  "aggregate": "avg",
  "state": "firing",
  "triggered_at": 1707350400,
  "url": "chart?metric=cpu_usage&host=web-1&from=-1h&theme=auto"
}
```

The `url` field is a relative path to an SVG chart of the metric — useful for embedding in Slack messages, dashboards, or email notifications.

Webhooks are fire-and-forget with a 10-second timeout. Failed deliveries are logged but do not block alert evaluation.

## ISP Examples

### Interface utilization alert

```bash
curl -X POST http://localhost:8428/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Uplink saturation",
    "metric": "port_utilization",
    "labels": {"port": "ae0"},
    "condition": "above",
    "threshold": 85.0,
    "duration": 600,
    "aggregate": "avg",
    "webhook_url": "http://noc-bot:3000/webhook"
  }'
```

### DHCP pool running low

```bash
curl -X POST http://localhost:8428/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "DHCP pool exhaustion",
    "metric": "dhcp_leases",
    "labels": {"pool": "residential"},
    "condition": "above",
    "threshold": 3685,
    "duration": 0,
    "aggregate": "last",
    "webhook_url": "http://noc-bot:3000/webhook"
  }'
```

(3685 = 90% of a /22 pool with 4094 usable addresses)

### Device unreachable (no data)

Monitor for metrics dropping below expected values:

```bash
curl -X POST http://localhost:8428/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Device offline",
    "metric": "snmp_uptime",
    "labels": {"device": "core-rtr-1"},
    "condition": "below",
    "threshold": 1.0,
    "duration": 900,
    "aggregate": "count",
    "webhook_url": "http://noc-bot:3000/webhook"
  }'
```

### Connecting to ntfy.sh

[ntfy](https://ntfy.sh) works out of the box — just point `webhook_url` at your topic. Since TimelessMetrics sends JSON, ntfy will accept it directly and you'll get push notifications on your phone or desktop:

```bash
curl -X POST http://localhost:8428/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Uplink saturation",
    "metric": "port_utilization",
    "labels": {"port": "ae0"},
    "condition": "above",
    "threshold": 85.0,
    "duration": 600,
    "aggregate": "avg",
    "webhook_url": "https://ntfy.sh/my-noc-alerts"
  }'
```

For richer notifications with titles and priority, route through a small transformer:

```elixir
# In your webhook receiver:
def handle_alert(payload) do
  priority = if payload["state"] == "firing", do: "high", else: "default"
  title = "#{String.upcase(payload["state"])}: #{payload["alert"]}"
  body = "#{payload["metric"]} #{payload["condition"]} #{payload["threshold"]} (current: #{payload["value"]})"

  :httpc.request(:post, {
    ~c"https://ntfy.sh/my-noc-alerts",
    [{~c"Title", String.to_charlist(title)},
     {~c"Priority", String.to_charlist(priority)},
     {~c"Tags", ~c"rotating_light"}],
    ~c"text/plain",
    String.to_charlist(body)
  }, [], [])
end
```

Or if you're self-hosting ntfy, point it at your instance:

```bash
"webhook_url": "https://ntfy.your-domain.com/noc-alerts"
```

### Connecting to Slack

Point the `webhook_url` at a Slack incoming webhook. The JSON payload works directly, or route through a lightweight transformer to format it as a Slack message with the embedded chart link:

```elixir
# In your webhook receiver (separate service):
def handle_alert(payload) do
  chart_url = "http://timeless:8428/#{payload["url"]}"

  Slack.post_message("#noc-alerts", """
  *#{payload["state"]}*: #{payload["alert"]}
  #{payload["metric"]} #{payload["condition"]} #{payload["threshold"]} (current: #{payload["value"]})
  <#{chart_url}|View Chart>
  """)
end
```

## Evaluation Timing

Alerts are evaluated on every rollup tick. The default rollup interval is **5 minutes**, configurable via the schema DSL:

```elixir
defmodule MySchema do
  use TimelessMetrics.Schema

  rollup_interval :timer.minutes(1)  # evaluate alerts every minute

  # ... tier definitions ...
end
```

The lookback window for each rule is `max(duration, 600)` seconds, ensuring at least 10 minutes of data is considered even for rules with `duration: 0`.
