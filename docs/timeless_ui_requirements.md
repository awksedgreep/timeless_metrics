# Timeless UI — Requirements

Separate hex package (`timeless_ui`) providing a Phoenix LiveView admin panel for Timeless. Follows the `phoenix_live_dashboard` pattern — optional, pluggable, zero impact on the core library.

## Package Design

- **Name**: `timeless_ui`
- **Depends on**: `timeless` (core library), `phoenix_live_view`
- **Install**: `{:timeless_ui, "~> 0.1"}` — people who don't want it, don't add it
- **Mount**: pluggable into any Phoenix router

```elixir
# In your Phoenix router:
import TimelessUI.Router

scope "/" do
  pipe_through [:browser, :admin_auth]
  timeless_dashboard "/timeless", store: :metrics
end
```

## Features

### Read-Only (config is infrastructure-as-code)

| Feature | Description |
|---|---|
| **Schema viewer** | Display rollup tiers, resolutions, retention policies, rollup interval |
| **Shard status** | Per-shard DB size, segment count, watermarks, last flush time |
| **Store health** | Series count, total points, storage bytes, bytes/point, buffer backlog |
| **Rollup status** | Last rollup time, tier watermarks, rows per tier, rollup duration |
| **Retention status** | Last cleanup time, rows purged, next scheduled run |

### Read-Write (operational data)

| Feature | Description |
|---|---|
| **Alert management** | Create/edit/delete alert rules, view current state per series, test webhooks |
| **Annotation management** | Create/delete annotations, view on timeline |
| **Metadata editor** | Register/update metric type, unit, description |

### Interactive

| Feature | Description |
|---|---|
| **Series browser** | List all metrics, filter by name/labels, view label cardinality |
| **Chart explorer** | Interactive metric charting with time range picker, aggregate selector, forecast/anomaly toggles |
| **Query builder** | Visual query constructor → shows equivalent curl/Elixir code |
| **Alert history** | Timeline of alert state transitions (firing → resolved) |

## Pages

1. **Dashboard** (`/timeless`) — overview: store health, active alerts, recent annotations, shard status summary
2. **Metrics** (`/timeless/metrics`) — series browser with search, label filters, cardinality stats
3. **Explorer** (`/timeless/explorer`) — interactive chart with controls for time range, aggregation, forecast, anomalies
4. **Alerts** (`/timeless/alerts`) — alert rule CRUD, current states, firing history, webhook test button
5. **Annotations** (`/timeless/annotations`) — annotation list with create/delete, timeline view
6. **Schema** (`/timeless/schema`) — read-only view of tiers, retention, rollup config
7. **Health** (`/timeless/health`) — per-shard details, DB sizes, watermarks, buffer stats

## Authentication

Two modes:

1. **Inherit from core** — if `TIMELESS_BEARER_TOKEN` is set, the UI requires the same token (passed via session cookie after initial auth)
2. **Phoenix auth pipeline** — the user's existing auth (Guardian, pow, etc.) via `pipe_through` in the router

The UI should never bypass the core library's token auth when making API calls internally.

## Technical Approach

- LiveView for real-time updates (alert states, buffer backlog, health stats)
- Server-rendered — no JS build step beyond LiveView's standard JS hooks
- Tailwind CSS via CDN or inline styles (no build pipeline)
- Charts: render using Timeless.Chart (the existing SVG renderer) embedded in LiveView
- All data access through the public `Timeless` API — no direct DB access

## API Surface Required from Core

The UI will need these functions from `timeless` (most already exist):

| Function | Status | Notes |
|---|---|---|
| `Timeless.info/1` | Exists | Store health stats |
| `Timeless.list_alerts/1` | Exists | Alert rules + states |
| `Timeless.create_alert/2` | Exists | |
| `Timeless.delete_alert/2` | Exists | |
| `Timeless.evaluate_alerts/1` | Exists | Manual trigger |
| `Timeless.annotations/3` | Exists | Query annotations |
| `Timeless.annotate/3` | Exists | Create annotation |
| `Timeless.get_metadata/2` | Exists | |
| `Timeless.register_metric/4` | Exists | |
| `Timeless.query_aggregate_multi/4` | Exists | For chart explorer |
| `Timeless.forecast/4` | Exists | |
| `Timeless.detect_anomalies/4` | Exists | |
| `Timeless.get_schema/1` | Exists | Read schema config |
| `Timeless.list_metrics/1` | **Needed** | List all metric names |
| `Timeless.list_series/2` | **Needed** | List series for a metric with labels |
| `Timeless.shard_stats/1` | **Needed** | Per-shard DB size, segment counts, watermarks |
| `Timeless.rollup_status/1` | **Needed** | Last rollup time, duration, rows processed |
| `Timeless.delete_annotation/2` | Exists | Via HTTP, may need Elixir API |

Functions marked **Needed** would be added to the core library before building the UI.

## Non-Goals (v1)

- User management / RBAC (use reverse proxy or Phoenix auth)
- Grafana datasource plugin (use the existing Prometheus-compatible endpoint)
- Log viewer (Timeless is metrics only)
- Distributed cluster management (future — see `docs/timeless_global_registry.md`)

## Release Strategy

- Separate hex package, separate git repo
- Own version cadence independent of `timeless` core
- Minimum `timeless` version pinned (e.g. `~> 0.3` once the needed API functions are added)
- Phoenix 1.7+ / LiveView 0.20+ required
