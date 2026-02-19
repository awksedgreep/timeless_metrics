# TimelessMetrics Global Registry (Future Project)

Design notes for a distributed ingest gateway that sits in front of a cluster
of TimelessMetrics nodes. Separate project from TimelessMetrics itself.

## Goal

Single entry point for metric ingestion across a cluster of TimelessMetrics nodes.
Pre-resolves series IDs at the gateway layer so every downstream node stays on
the `write_batch_resolved/2` hot path (~4M+ pts/sec per node).

**Target headline:** 50-100M pts/sec cluster ingestion (20-40 nodes).

## Architecture

```
Load Balancer (round-robin)
    ├──> Gateway 1 ──┐
    ├──> Gateway 2 ──┤    ETS: {metric, labels} → {node, series_id, replica_node}
    └──> Gateway N ──┘
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
      Node 1      Node 2      Node N
      (TimelessMetrics)  (Timeless)  (Timeless)
```

## Gateway Responsibilities

1. **Ingest** — accept HTTP requests (same VM JSON / Prometheus endpoints as TimelessMetrics)
2. **Resolve** — look up `{metric, labels} → {node, series_id}` in local ETS cache
3. **Assign** — on cache miss, pick an owner node (consistent hash or least-loaded), call `resolve_series/3` on it, cache the result
4. **Batch** — group entries by target node
5. **Forward** — send pre-resolved batches via `write_batch_resolved/2` over Erlang distribution (avoids HTTP serialization overhead)
6. **Route queries** — proxy reads to the owning node (no fan-out needed)

## Key Design Decisions

### Gateways Are Stateless

The ETS routing cache is rebuildable from the nodes' series registries on
startup. Lose a gateway, spin up another — it warms its cache from the nodes
in seconds. Run as many gateways as needed behind a load balancer.

### Nodes Are Independent

TimelessMetrics nodes don't know about each other. No Erlang clustering between nodes.
Only gateways connect to nodes via Erlang distribution. Nodes are just standard
TimelessMetrics containers.

### No Query Fan-Out

Each series has exactly one owner node (plus optional replica). The gateway
knows the owner, so queries route directly — no distributed merge, no
coordination, no scatter-gather.

### Replication for HA

Gateway writes to both primary and replica for each series. On primary failure,
gateway flips reads to the replica. No consensus protocol needed — the gateway
owns the routing table and makes failover decisions.

## Sharding Strategy (To Be Tested)

Several options to explore:

- **Consistent hashing** on metric+labels — automatic, even distribution, but
  series move when nodes are added/removed. Good default.
- **Tag-based routing** — e.g., all `region=east` goes to node 1. Simple,
  predictable, manual. Good for logical separation.
- **Tiered registries** — hierarchical routing for very large clusters.
- **Hybrid** — consistent hash by default, tag-based overrides for specific
  metrics or label patterns.

Multiple nodes per series (primary + replica) for HA regardless of strategy.

## Node Discovery

- **DNS** for initial discovery — fits Podman/container deployment model
- **Erlang distribution** between gateways and nodes for internal communication
  (`:net_adm.connect_node/1` after DNS resolution)
- Gateways discover nodes, pull their series registries, build local ETS cache

## Bottleneck Analysis

At 50M pts/sec the bottleneck chain is:

1. **HTTP parsing** — OTP `:json.decode` (C NIF) is fast but there's a limit.
   Multiple gateways behind a load balancer solves this.
2. **ETS lookups** — millions of entries, concurrent reads, near-zero overhead.
   Not a bottleneck.
3. **Erlang distribution** — binary term format is efficient but has per-node
   throughput limits. Batching amortizes this. May need benchmarking at scale.
4. **Node write throughput** — proven at 2.5M pts/sec per node over HTTP,
   should be faster over distribution (no HTTP overhead).

## Project Structure

Separate Elixir project (`timeless_gateway`), likely structured as:

- `TimelessGateway.Router` — HTTP endpoints (same API surface as TimelessMetrics.HTTP)
- `TimelessGateway.Registry` — ETS cache of series → node mappings
- `TimelessGateway.Discovery` — DNS-based node discovery + Erlang connect
- `TimelessGateway.Forwarder` — batches and sends pre-resolved writes to nodes
- `TimelessGateway.HealthCheck` — monitors node liveness, triggers failover

Depends on TimelessMetrics as a library (for `resolve_series/3`, `write_batch_resolved/2`
type specs) but does not start a local TimelessMetrics instance.

## Open Questions

- Rebalancing: what happens when a node is added? Gradual migration of series
  or let new series land on the new node and old ones stay put?
- Cache invalidation: how does a gateway learn about series created by another
  gateway on the same node?
- Backpressure: if a node falls behind, should the gateway buffer, drop, or
  redirect to the replica?
- Metrics about metrics: the gateway itself should expose throughput/latency
  stats, probably via its own TimelessMetrics instance.
