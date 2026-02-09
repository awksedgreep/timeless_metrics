# Timeless — Next Steps

## Future

### timeless_ui
Separate Phoenix LiveView package for admin/monitoring. Requirements captured in `docs/timeless_ui_requirements.md`. Prototype started — embeds Timeless as a dependency, runs on port 4001.

## Completed

- **Alert rule retention** — Orphaned rules (metric has no series) auto-deleted during retention enforcement
- **Test coverage for pending segment flush** — 3 tests in `test/pending_flush_test.exs`
- **`info()` undercounting** — Now includes `pending_points` from SegmentBuilder memory
- **`pending_flush_interval` configurable** — Wired through supervisor, application.ex, and `TIMELESS_PENDING_FLUSH_INTERVAL` env var
- **Import error reporting** — Server-side Logger.warning with sample failed lines, response includes `failed_lines` array
- **Historical data backfill script** — `examples/backfill.exs` generates 7 days of realistic ISP data with daily/weekly patterns, reboots, SNR events, traffic spikes
- **Stale plan file cleanup** — Deleted `.claude/plans/quiet-watching-cocoa.md`
