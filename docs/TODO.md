# Timeless — Next Steps

## Bugs & Gaps

### Alert rule retention worker
Self-cleaning state rows are done (state deleted on `ok` transition), but there's no mechanism to prune old or orphaned alert **rules**. Plan: add `TIMELESS_ALERT_RETENTION` env var and a periodic worker that deletes rules with no matching series or rules that haven't fired in N days.

## Tooling

### Historical data backfill script
The real-time load generator (`/tmp/timeless_load.sh`) is useful for live testing, but for UI development a script that backfills days/weeks of historical data with realistic patterns would be more valuable. Include device reboots (uptime resets), traffic spikes, and SNR degradation events.

## Future

### timeless_ui
Separate Phoenix LiveView package for admin/monitoring. Requirements captured in `docs/timeless_ui_requirements.md`. Prototype started — embeds Timeless as a dependency, runs on port 4001.

### Clean up stale plan file
The bearer token auth plan at `.claude/plans/quiet-watching-cocoa.md` is fully implemented and can be deleted.

## Completed

- **Test coverage for pending segment flush** — 3 tests in `test/pending_flush_test.exs`
- **`info()` undercounting** — Now includes `pending_points` from SegmentBuilder memory
- **`pending_flush_interval` configurable** — Wired through supervisor, application.ex, and `TIMELESS_PENDING_FLUSH_INTERVAL` env var
- **Import error reporting** — Server-side Logger.warning with sample failed lines, response includes `failed_lines` array
