# Mustang: Stats API Changes

## 1. Context

OpenSearch has native (Rust-based) components for both reads and writes. The read plugin (DataFusion) handles range queries and aggregations via a Rust/Tokio-based query execution engine running as a parallel search path alongside Lucene. The write module (Parquet) implements a dual-write architecture where every indexed document is written to both Lucene and Parquet in parallel.

The stats APIs (`_nodes/stats`, `{index}/_stats`, `_cluster/stats`) need to reflect the native components' resource usage, memory consumption, and operational metrics. Currently, neither DataFusion's Tokio runtime stats nor the Parquet write module's native memory and file metadata are visible to the stats APIs.

---

## 2. Current State

### 2.1 General

#### Plugin landscape

Two native components exist in the codebase:

```
Search path (per-shard):
SearchService.executeQueryPhase()
    ├──► LuceneEngine (existing)              → Lucene query path
    └──► DatafusionEngine (SearchExecEngine)  ← DataFusion read path
             ├── createContext()
             ├── executeQueryPhase()           → Rust/Tokio async execution
             └── executeFetchPhase()

Write path (per-shard):
CompositeEngine.indexDocument()
    ├──► LuceneEngine.indexDocument()          → Lucene segment files
    └──► ParquetExecutionEngine.index()        ← Parquet write path
             └── Arrow RecordBatch (native memory) → Parquet files on flush
```

- **DataFusion details**: Implements `SearchEnginePlugin` → `SearchExecEngine` per shard. On the search path, `DatafusionEngine.executeQueryPhase()` dispatches range queries and aggregations to the Rust/Tokio async runtime, and `executeFetchPhase()` retrieves results. This is a parallel search path alongside Lucene — not a replacement. The Rust side maintains Tokio runtime stats (task counts, poll times, worker-level memory) but none of these are currently exposed to any stats API. No instrumentation bridge exists today between the native runtime and `_nodes/stats`.
- **Parquet details**: The `ParquetExecutionEngine` sits behind `CompositeEngine`. On every `indexDocument()`, the document is written to both Lucene and Parquet. The Parquet side maintains its own native write buffers (Arrow RecordBatches in Rust memory), file metadata (row groups, column chunks), and periodic flush/merge operations. None of these are currently visible in any stats API.

#### Indexing path (dual-write architecture)

```
DocumentWrite
    │
    ▼
CompositeEngine.indexDocument()
    ├──► LuceneEngine.indexDocument()     → Lucene segment files
    └──► ParquetExecutionEngine.index()   → Arrow RecordBatch (native memory)
                                               │
                                               ▼ (on flush/refresh)
                                          Parquet files on disk
```

Every indexed document goes through `CompositeEngine`, which fans out to both engines. The Lucene path is unchanged. The Parquet path accumulates documents in native (Rust) memory as Arrow RecordBatches, then flushes to Parquet files during refresh operations.

### 2.2 Stats API Architecture

#### Three APIs, shared infrastructure

All three stats APIs follow the same pattern: REST endpoint → Transport action → Collection from local services → Aggregation → Serialization.

| API | REST Endpoint | Transport Action | Scope |
|---|---|---|---|
| Node Stats | `GET _nodes/stats` | `TransportNodesStatsAction` | Per-node metrics (OS, JVM, thread pools, indexing, search, etc.) |
| Index Stats | `GET {index}/_stats` | `TransportIndicesStatsAction` | Per-shard/per-index metrics (docs, store, segments, etc.) |
| Cluster Stats | `GET _cluster/stats` | `TransportClusterStatsAction` | Cluster-wide aggregation of node + index metrics |

#### Shared components

- **`NodeService.stats()`**: Central collection point for node-level metrics. Calls into `MonitorService`, `IndicesService`, etc.
- **`CommonStats`**: Shared container for shard-level stats used by both Index Stats and Cluster Stats APIs.
- **`IndexShard`**: Each shard collects its own stats; these are aggregated up to index and cluster level.

#### Node Stats: node-level metrics

| Stats | Source | Metrics | What It Tells You |
|---|---|---|---|
| `os` | OS | • `cpu.percent`<br>• `cpu.load_average.*`<br>• `mem.total_in_bytes`<br>• `mem.free_in_bytes`<br>• `mem.used_in_bytes`<br>• `mem.free_percent`<br>• `mem.used_percent`<br>• `swap.total_in_bytes`<br>• `swap.free_in_bytes`<br>• `swap.used_in_bytes`<br>• `cgroup.cpuacct.*`<br>• `cgroup.cpu.*`<br>• `cgroup.memory.*` | System-wide resource snapshot. "Is the box running hot?" — CPU load, RAM pressure, swap usage. Cgroup stats matter for containerized deployments (e.g., k8s pods with CPU/memory limits). |
| `process` | OS | • `timestamp`<br>• `open_file_descriptors`<br>• `max_file_descriptors`<br>• `cpu.percent`<br>• `cpu.total_in_millis`<br>• `mem.total_virtual_in_bytes` | The OpenSearch JVM process itself. File descriptor exhaustion is a common failure mode — if `open_file_descriptors` approaches `max`, you're in trouble. Virtual memory shows the full address space footprint. |
| `jvm` | JVM | • `mem.heap_used_in_bytes`<br>• `mem.heap_used_percent`<br>• `mem.heap_committed_in_bytes`<br>• `mem.heap_max_in_bytes`<br>• `mem.non_heap_used_in_bytes`<br>• `mem.non_heap_committed_in_bytes`<br>• `mem.pools.*`<br>• `threads.count`<br>• `threads.peak_count`<br>• `gc.collectors.{name}.collection_count`<br>• `gc.collectors.{name}.collection_time_in_millis`<br>• `buffer_pools.{name}.count`<br>• `buffer_pools.{name}.used_in_bytes`<br>• `buffer_pools.{name}.total_capacity_in_bytes`<br>• `classes.current_loaded_count`<br>• `classes.total_loaded_count`<br>• `classes.total_unloaded_count` | JVM internals via MXBeans. Heap at 75%+ with frequent old-gen GC? Time to tune. Buffer pools track direct/mapped byte buffers (Lucene's MMapDirectory lives here). Native Rust memory is off-heap and invisible to these stats. |
| `thread_pool` | OpenSearch | • `{pool_name}.threads`<br>• `{pool_name}.queue`<br>• `{pool_name}.active`<br>• `{pool_name}.rejected`<br>• `{pool_name}.largest`<br>• `{pool_name}.completed` | OpenSearch's own thread pools (WRITE, SEARCH, REFRESH, etc.). Rising `rejected` means the pool can't keep up. Parquet writes use the WRITE pool, refresh uses REFRESH — so these pools already capture native write path activity. DataFusion's Tokio threads are invisible here. |
| `fs` | OS | • `total.total_in_bytes`<br>• `total.free_in_bytes`<br>• `total.available_in_bytes`<br>• `data[].total_in_bytes`<br>• `data[].free_in_bytes`<br>• `data[].available_in_bytes`<br>• `io_stats.devices[].operations`<br>• `io_stats.devices[].read_operations`<br>• `io_stats.devices[].write_operations`<br>• `io_stats.devices[].read_kilobytes`<br>• `io_stats.devices[].write_kilobytes` | Disk space and I/O via `FileStore` syscalls + `/proc/diskstats` (Linux). Parquet files sit on the same data paths, so their disk usage is automatically reflected. Cached with 1s refresh via `SingleObjectCache`. |
| `transport` | OpenSearch | • `server_open`<br>• `total_outbound_connections`<br>• `rx_count`<br>• `rx_size_in_bytes`<br>• `tx_count`<br>• `tx_size_in_bytes` | Inter-node communication (the transport layer, not HTTP). How much data is flowing between nodes for shard replication, search fan-out, cluster state, etc. |
| `http` | OpenSearch | • `current_open`<br>• `total_opened` | Client-facing REST connections. A spike in `current_open` might mean clients aren't closing connections properly. |
| `breaker` | OpenSearch | • `{breaker_name}.limit_size_in_bytes`<br>• `{breaker_name}.estimated_size_in_bytes`<br>• `{breaker_name}.overhead`<br>• `{breaker_name}.tripped` | Circuit breakers prevent OOM. Built-in: `parent` (real JVM heap check), `fielddata` (40% heap), `request` (60% heap), `in_flight_requests` (100% heap). Plugins register custom child breakers via `CircuitBreakerPlugin` — they auto-appear here. `tripped > 0` means requests got rejected to protect the node. |
| `script` | OpenSearch | • `compilations`<br>• `cache_evictions`<br>• `compilation_limit_triggered` | Painless/scripting engine. High `compilations` with `compilation_limit_triggered > 0` means scripts aren't being cached — likely dynamic scripts without params. |
| `discovery` | OpenSearch | • `cluster_state_queue.total`<br>• `cluster_state_queue.pending`<br>• `cluster_state_queue.committed`<br>• `published_cluster_states.full_states`<br>• `published_cluster_states.compatible_diffs`<br>• `published_cluster_states.incompatible_diffs` | Cluster coordination health. Growing `pending` queue means the node can't apply cluster state updates fast enough. `incompatible_diffs` force full state transfers — expensive. |
| `ingest` | OpenSearch | • `total.count`<br>• `total.time_in_millis`<br>• `total.current`<br>• `total.failed`<br>• `pipelines.{name}.count`<br>• `pipelines.{name}.time_in_millis`<br>• `pipelines.{name}.current`<br>• `pipelines.{name}.failed`<br>• `pipelines.{name}.processors[].{type}.count/time_in_millis/current/failed` | Document transformation pipelines (grok, date, convert, etc.) that run BEFORE documents reach the engine. Entirely JVM-side, upstream of both Lucene and Parquet. Per-processor breakdown helps find slow processors. |
| `adaptive_selection` | OpenSearch | • `{node_id}.outgoing_searches`<br>• `{node_id}.avg_response_time_ns`<br>• `{node_id}.avg_service_time_ns`<br>• `{node_id}.avg_queue_size`<br>• `{node_id}.rank` | Smart search routing. The coordinating node tracks EWMA of response time, service time, and queue size per data node, then ranks them using the C3 paper formula. Lower rank = preferred node. DataFusion may change response times but the routing mechanism itself is unchanged. |
| `script_cache` | OpenSearch | • `{context}.compilations`<br>• `{context}.cache_evictions`<br>• `{context}.compilation_limit_triggered` | Per-context breakdown of script caching (e.g., `score`, `ingest`, `update`). Same data as `script` but split by execution context. |
| `indexing_pressure` | OpenSearch | • `memory.current.coordinating_in_bytes`<br>• `memory.current.primary_in_bytes`<br>• `memory.current.replica_in_bytes`<br>• `memory.current.all_in_bytes`<br>• `memory.total.coordinating_in_bytes`<br>• `memory.total.primary_in_bytes`<br>• `memory.total.replica_in_bytes`<br>• `memory.total.all_in_bytes`<br>• `memory.limit_in_bytes` | Node-level memory accounting for in-flight indexing operations. Tracks bytes at each stage (coordinating → primary → replica). When `all_in_bytes` approaches `limit_in_bytes`, new requests get rejected. This is where native write buffer memory needs to be added. |
| `shard_indexing_pressure` | OpenSearch | • `stats.{shard_id}.memory.current_in_bytes`<br>• `stats.{shard_id}.memory.total_in_bytes`<br>• `stats.{shard_id}.rejection.coordinating.*`<br>• `stats.{shard_id}.rejection.primary.*`<br>• `stats.{shard_id}.rejection.replica.*`<br>• `stats.{shard_id}.last_successful_timestamp` | Same as `indexing_pressure` but per-shard. Helps identify hot shards that are consuming disproportionate memory. Rejection counts show which shards are getting throttled. |
| `search_backpressure` | OpenSearch | • `search_task.cancellation_count`<br>• `search_task.limit_reached_count`<br>• `search_task.current_query_count`<br>• `search_shard_task.cancellation_count`<br>• `search_shard_task.limit_reached_count`<br>• `search_shard_task.current_query_count` | `SearchBackpressureService` cancels expensive queries to protect the node. `cancellation_count > 0` means queries are being killed. Tracks both full search tasks and per-shard tasks. Native resource consumption needs to feed into these cancellation decisions. |
| `cluster_manager_throttling` | OpenSearch | • `stats.total_throttled_tasks`<br>• `stats.throttled_pending_task_count` | Cluster manager (master) task queue throttling. If the master is overwhelmed with pending tasks (index creation, mapping updates, shard allocation), it throttles. Only relevant on master-eligible nodes. |
| `weighted_routing` | OpenSearch | • `stats.fail_open_count` | Weighted shard routing for AZ-aware traffic shaping. `fail_open_count` tracks how often routing fell back to non-preferred AZs because preferred ones were unavailable. |
| `search_pipeline` | OpenSearch | • `total_request.count`<br>• `total_request.time_in_millis`<br>• `total_request.current`<br>• `total_request.failed`<br>• `total_response.count`<br>• `total_response.time_in_millis`<br>• `total_response.current`<br>• `total_response.failed`<br>• `pipelines.{name}.*` | Search request/response transformation pipelines (like ingest pipelines but for search). Processors can rewrite queries or post-process results. Runs on JVM, upstream of DataFusion. |
| `task_cancellation` | OpenSearch | • `search_shard_task.current_count_post_cancel`<br>• `search_shard_task.total_count_post_cancel` | Tracks search shard tasks that are still running after cancellation was requested. High `current_count_post_cancel` means cancelled tasks aren't cleaning up promptly. All cancellation happens on JVM; native blindly executes. |
| `resource_usage_stats` | OpenSearch | • `{node_id}.timestamp`<br>• `{node_id}.cpu_utilization_percent`<br>• `{node_id}.memory_utilization_percent`<br>• `{node_id}.io_usage_stats.*` | `ResourceUsageCollectorService` periodically samples node resource usage and shares it across the cluster. Used by `SearchBackpressureService` and admission control for load-based decisions. Native resource usage needs to feed into this via `ServiceCache`. |
| `segment_replication_backpressure` | OpenSearch | • `total_rejected_requests` | Segment replication (replica shards pull segments from primary). When replicas fall too far behind, new indexing requests get rejected. Lucene-segment-based — Parquet file replication is separate (future work). |
| `repositories` | OpenSearch | • `{repo_name}.request_counts.*`<br>• `{repo_name}.exceptions.*`<br>• `{repo_name}.throttles.*`<br>• `{repo_name}.latency.*` | Snapshot repository I/O stats (S3, Azure, GCS, etc.). Tracks every blob store operation — GetObject, PutObject, ListObjects, etc. Also used for remote store segment/translog uploads. All via Java SDK clients. |
| `admission_control` | OpenSearch | • `{controller_name}.transport.{action}.rejection_count` | Admission controllers reject requests before they consume resources. E.g., if CPU is at 95%, new search requests get rejected at the door. Decisions happen on JVM; native resource usage may inform decisions but the stats structure doesn't change. |
| `caches` | OpenSearch | • `{cache_type}.size_in_bytes`<br>• `{cache_type}.evictions`<br>• `{cache_type}.hit_count`<br>• `{cache_type}.miss_count`<br>• `{cache_type}.item_count` | Pluggable tiered cache framework (experimental). Backed by `CacheService` → `NodeCacheStats`. Currently only `INDICES_REQUEST_CACHE` (request cache) is registered. `CacheType` is a fixed enum — not plugin-extensible. Supports on-heap and disk-based (Ehcache) store implementations. Supports dimensional breakdown by shard/tier via `levels` query param. DataFusion has its own Rust-side cache that's invisible here (see §4.2). |
| `remote_store_node_stats` | OpenSearch | • `last_successful_fetch_of_pinned_timestamps`<br>• `segment.last_successful_upload_timestamp`<br>• `segment.last_failed_upload_timestamp`<br>• `translog.last_successful_upload_timestamp`<br>• `translog.last_failed_upload_timestamp`<br>• `translog.download_stats.*` | Remote store health. Timestamps show when the last segment/translog upload succeeded or failed. Growing gap between `last_successful` and now means uploads are stalling. |

#### Index Stats: shard-level metrics

| Stats | Source | Metrics | What It Tells You |
|---|---|---|---|
| `docs` | Lucene | • `count`<br>• `deleted` | How many documents are in the shard (Lucene's view). `deleted` are docs marked for deletion but not yet merged away — high `deleted` count means merges are behind. |
| `store` | Lucene | • `size_in_bytes`<br>• `reserved_in_bytes` | On-disk size of Lucene segment files. `reserved_in_bytes` is space held for in-progress merges. Doesn't include Parquet files — those go under `native_store` (see §5.2). |
| `indexing` | OpenSearch | • `index_total`<br>• `index_time_in_millis`<br>• `index_current`<br>• `index_failed`<br>• `delete_total`<br>• `delete_time_in_millis`<br>• `delete_current`<br>• `noop_update_total`<br>• `is_throttled`<br>• `throttle_time_in_millis`<br>• `doc_status.*` | Indexing throughput and health. Collected at `IndexShard` level via `InternalIndexingStats` (an `IndexingOperationListener`), upstream of the engine — so both Lucene and Parquet writes are already captured. `is_throttled` + `throttle_time` show merge-related backpressure. |
| `get` | OpenSearch | • `total`<br>• `time_in_millis`<br>• `exists_total`<br>• `exists_time_in_millis`<br>• `missing_total`<br>• `missing_time_in_millis`<br>• `current` | Get-by-ID (real-time GET) performance. Goes through Lucene only — DataFusion handles range queries/aggregations, not point lookups. |
| `search` | OpenSearch | • `open_contexts`<br>• `query_total`<br>• `query_time_in_millis`<br>• `query_current`<br>• `query_failed`<br>• `concurrent_query_total`<br>• `concurrent_query_time_in_millis`<br>• `concurrent_query_current`<br>• `concurrent_avg_slice_count`<br>• `fetch_total`<br>• `fetch_time_in_millis`<br>• `fetch_current`<br>• `scroll_total`<br>• `scroll_time_in_millis`<br>• `scroll_current`<br>• `point_in_time_total`<br>• `point_in_time_time_in_millis`<br>• `point_in_time_current`<br>• `suggest_total`<br>• `suggest_time_in_millis`<br>• `suggest_current`<br>• `search_idle_reactivate_count_total` | Search throughput. Collected at JVM level — DataFusion execution time is included in `query_time_in_millis`. `open_contexts` shows active search contexts (scrolls, PIT). `concurrent_*` tracks concurrent segment search (slice-level parallelism). |
| `merge` | Lucene | • `current`<br>• `current_docs`<br>• `current_size_in_bytes`<br>• `total`<br>• `total_time_in_millis`<br>• `total_docs`<br>• `total_size_in_bytes`<br>• `total_stopped_time_in_millis`<br>• `total_throttled_time_in_millis`<br>• `total_auto_throttle_in_bytes`<br>• `unreferenced_file_cleanups_performed` | Lucene segment merges. Small segments get merged into larger ones for query efficiency. `total_throttled_time` shows how much merge I/O was throttled to avoid starving searches. Parquet compaction is a separate operation (see OQ8). |
| `refresh` | OpenSearch | • `total`<br>• `total_time_in_millis`<br>• `external_total`<br>• `external_total_time_in_millis`<br>• `listeners` | How often and how fast new documents become searchable. `external_total` counts refreshes visible to search. With Parquet, refresh also triggers native flush (RecordBatch → Parquet file), so `total_time_in_millis` now includes native flush time. |
| `flush` | OpenSearch | • `total`<br>• `periodic`<br>• `total_time_in_millis` | Lucene translog flush (commit to disk for durability). Different from refresh — flush is about durability, refresh is about visibility. Parquet flush is tied to refresh, not translog flush. |
| `warmer` | OpenSearch | • `current`<br>• `total`<br>• `total_time_in_millis` | Index warming — pre-loading data structures after a new segment is opened (e.g., global ordinals for fielddata). Lucene-specific. |
| `query_cache` | OpenSearch | • `memory_size_in_bytes`<br>• `total_count`<br>• `hit_count`<br>• `miss_count`<br>• `cache_size`<br>• `cache_count`<br>• `evictions` | Per-shard query cache (caches filter results as bitsets). High `hit_count` / low `miss_count` = good cache utilization. JVM heap-based. DataFusion has its own caching on the Rust side (not exposed here). |
| `fielddata` | OpenSearch | • `memory_size_in_bytes`<br>• `evictions`<br>• `fields.{name}.memory_size_in_bytes` | JVM heap used for fielddata (uninverted index for sorting/aggregations on text fields). Per-field breakdown helps find which fields are eating heap. Mostly replaced by doc_values, but still used for text fields. |
| `completion` | Lucene | • `size_in_bytes`<br>• `fields.{name}.size_in_bytes` | Completion suggester (autocomplete) data structures in Lucene. Entirely Lucene-specific. |
| `segments` | Lucene | • `count`<br>• `memory_in_bytes`<br>• `terms_memory_in_bytes`<br>• `stored_fields_memory_in_bytes`<br>• `term_vectors_memory_in_bytes`<br>• `norms_memory_in_bytes`<br>• `points_memory_in_bytes`<br>• `doc_values_memory_in_bytes`<br>• `index_writer_memory_in_bytes`<br>• `version_map_memory_in_bytes`<br>• `fixed_bit_set_memory_in_bytes`<br>• `max_unsafe_auto_id_timestamp`<br>• `file_sizes.*` | Lucene segment internals. `count` = number of segments (too many = merges behind). Memory breakdown shows what's loaded in heap (terms dict, stored fields, etc.). Parquet row groups are analogous but go under `native_segments` (see §5.2). |
| `translog` | OpenSearch | • `operations`<br>• `size_in_bytes`<br>• `uncommitted_operations`<br>• `uncommitted_size_in_bytes`<br>• `earliest_last_modified_age`<br>• `remote_store.*` | Write-ahead log for durability. `uncommitted_operations` shows how much data would be lost on crash before next flush. Parquet has its own durability model (not translog-based). |
| `request_cache` | OpenSearch | • `memory_size_in_bytes`<br>• `evictions`<br>• `hit_count`<br>• `miss_count` | Caches full search responses for identical repeated queries. Only works for `size: 0` requests (no hits, just aggregations). JVM heap-based. |
| `recovery` | OpenSearch | • `current_as_source`<br>• `current_as_target`<br>• `throttle_time_in_millis` | Shard recovery (peer recovery, snapshot restore). `current_as_source` = this shard is sending data to a recovering replica. `throttle_time` shows I/O throttling during recovery. Lucene-segment-based — Parquet file recovery is separate (future work). |

#### Cluster Stats

Cluster Stats aggregates both node-level and index-level metrics:

**Node-level** (10 sections): `os`, `process`, `jvm`, `fs`, `plugins`, `network_types`, `discovery_types`, `packaging_types`, `ingest`, `node_count`

**Index-level** (9 sections via `ClusterStatsIndices`): `count`, `shards`, `docs`, `store`, `fielddata`, `query_cache`, `completion`, `segments`, `mappings`

---

## 3. Metric Update Strategies

When exposing native (Rust-side) metrics alongside existing JVM metrics, there are three possible approaches. Each has different trade-offs for backward compatibility, consumer impact, and operational clarity.

Note on terminology: a **stat** is a top-level grouping in the stats API response (e.g., `indexing_pressure`, `os`). A **metric** is an individual data point within a stat (e.g., `primary_in_bytes`, `cpu.percent`).

### 3.1 Option A: Native-only metrics (additive)

Add new native-specific metrics alongside existing metrics. Existing metrics remain JVM-only and unchanged. Consumers who want a combined view sum the metrics themselves.

```json
{
  "indexing_pressure": {
    "memory": {
      "current": {
        # Existing metrics
        "primary_in_bytes": 524288000,
        "replica_in_bytes": 0,
        "all_in_bytes": 524288000,

        # New metrics
        "native_primary_in_bytes": 67108864,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 67108864
      }
    }
  }
}
```

Pros:

* Zero risk to existing consumers — no metric semantics change.
* No need to audit all metric consumers before shipping.
* Internal enforcement paths (rejection, backpressure) continue to work unchanged.
* Purely additive — can evolve to Option B or C later without breaking anything.

Cons:

* Consumers who want a combined JVM+native number must sum manually.
* Dashboards that want "total memory" need to be updated to add the new metrics.

### 3.2 Option B: Native metrics + summed aggregates

Add native metrics and redefine existing aggregate metrics to be JVM+native sums. The JVM portion is derived: `jvm = total - native`.

```json
{
  "indexing_pressure": {
    "memory": {
      "current": {
        # Existing metrics modified to include JVM + native
        "primary_in_bytes": 591396864,
        "replica_in_bytes": 0,
        "all_in_bytes": 591396864,

        # Native only metrics
        "native_primary_in_bytes": 67108864,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 67108864
      }
    }
  }
}
```

Pros:

* Existing aggregate metrics give a "total memory" view out of the box.
* Backward-compatible in shape — same metric names, same JSON structure.

Cons:

* Silently changes the meaning of existing metrics — they now include off-heap native memory.
* Breaks internal enforcement paths that compare these values against JVM-heap-derived limits.
* Requires auditing ALL consumers (internal, AWS-internal, external) before shipping.
* JVM portion requires derivation (`total - native`), not explicit.

### 3.3 Option C: Three-way breakdown (native + JVM + summed aggregates)

Add both native and JVM breakdown metrics, redefine existing aggregates as sums. Every value is explicit: `primary_in_bytes = jvm_primary_in_bytes + native_primary_in_bytes`.

```json
{
  "indexing_pressure": {
    "memory": {
      "current": {
        # Existing metrics modified to include JVM + native
        "primary_in_bytes": 591396864,
        "replica_in_bytes": 0,
        "all_in_bytes": 591396864,

        # Existing metrics but renamed
        "jvm_primary_in_bytes": 524288000,
        "jvm_replica_in_bytes": 0,
        "jvm_all_in_bytes": 524288000,

        # New native metrics
        "native_primary_in_bytes": 67108864,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 67108864
      }
    }
  }
}
```

Pros:

* Most explicit — no derivation needed, every value is directly available.
* Aggregate metrics give "total memory" view, breakdown metrics give JVM vs native.

Cons:

* Same semantic change problem as Option B — existing metrics now include native memory.
* Same consumer audit requirement as Option B.
* Most verbose — doubles the number of metrics per stat.
* Breaks internal enforcement paths the same way Option B does.

### 3.4 Option D: Native-only metrics + pre-computed totals (additive)

Keep existing metrics as JVM-only (unchanged), add new `native_*` metrics, and also add `total_*` metrics that are the pre-computed sum of existing + native. Every value is explicit and no existing metric changes semantics.

```json
{
  "indexing_pressure": {
    "memory": {
      "current": {
        # Existing metrics stay as is
        "primary_in_bytes": 524288000,
        "replica_in_bytes": 0,
        "all_in_bytes": 524288000,

        # New metrics
        "native_primary_in_bytes": 67108864,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 67108864,

        # Sumed metrics as new fields for easy consumption
        "total_primary_in_bytes": 591396864,
        "total_replica_in_bytes": 0,
        "total_all_in_bytes": 591396864
      }
    }
  }
}
```

Pros:

* Zero risk to existing consumers — existing metrics remain JVM-only, no semantic change.
* No need to audit all metric consumers before shipping.
* Internal enforcement paths (rejection, backpressure) continue to work unchanged.
* Consumers who want a combined JVM+native view get it out of the box via `total_*` — no client-side math needed.
* Purely additive — can evolve or deprecate individual prefixes later without breaking anyone.
* Forward-compatible — if existing JVM-only metrics are later deprecated, `total_*` is already in place.

Cons:

* Triples the number of metrics per stat (existing + native + total). For `indexing_pressure`, 6 metrics under `current` become 18. Response size increase is modest (a few KB across all stats) but the JSON is more verbose.
* Consumers setting alarms on `total_*` metrics need to understand that enforcement (rejection, backpressure) is driven by the JVM-only metrics, not the totals. A `total_*` alarm may fire while the JVM-only enforcement threshold hasn't been reached — this gap between observability and enforcement needs to be documented.

### 3.5 Analysis

The challenge with Options B and C is that summing changes the semantics of existing metrics. This affects three categories of consumers:

1. **Internal to OpenSearch** — rejection logic (`IndexingPressure`), backpressure, admission control. These consumers use the raw counters for enforcement decisions, not the stats API. If existing metrics suddenly include native bytes, enforcement thresholds (e.g., 10% of JVM heap) would be checked against a number that includes off-heap memory — physically meaningless.

2. **External to OpenSearch but internal to AWS** — Consumers like BeagleRock, BeagleStone, Carnaval alarms, etc, read `_nodes/stats` JSON and have thresholds calibrated to JVM-only values. Summing silently inflates the numbers, potentially triggering false alarms.

3. **External to AWS and OpenSearch** — community users, third-party monitoring tools (Datadog, Grafana, etc.), custom scripts. Same silent inflation problem. No way to notify all consumers of the semantic change.

Identifying and updating ALL consumers across these three categories before summing is a prerequisite for Options B and C. Missing even one consumer means silent breakage.

Option D avoids the semantic change problem entirely (like Option A) while also providing pre-computed totals for consumers who want a combined view. The trade-off is verbosity — three values per metric instead of two (Option A) or one (Option B). The enforcement/observability gap (alarms on `total_*` vs enforcement on JVM-only metrics) is inherent to any approach that keeps enforcement JVM-only, but with Option D it's more visible because `total_*` metrics explicitly invite combined monitoring.

#### Example: `indexing_pressure` and backpressure

`indexing_pressure` illustrates why summing breaks internal consumers. The rejection logic in `IndexingPressure` works like this:

```java
// IndexingPressure.markPrimaryOperationStarted()
long combinedBytes = this.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
long replicaWriteBytes = this.currentReplicaBytes.get();
long totalBytes = combinedBytes + replicaWriteBytes;
if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
    // reject — limit is 10% of JVM heap
    throw new OpenSearchRejectedExecutionException(...);
}
```

The limit (`primaryAndCoordinatingLimits`) is derived from `indexing_pressure.memory.limit` which defaults to 10% of JVM heap. If we sum native bytes into `currentCombinedCoordinatingAndPrimaryBytes`, the check becomes:

```
(jvm_bytes + native_bytes) > 10% of JVM heap
```

This is physically incorrect — native memory lives off-heap and has no relationship to JVM heap capacity. The rejection threshold was calibrated for JVM-only memory; inflating it with off-heap bytes makes the backpressure mechanism fire prematurely and unpredictably. Instead, it's better to revisit all consumers and update them as we anyway have to validate them for correctness with Mustang.


### 3.6 Recommendation

**Option A (additive native-only metrics)** is the safest starting point:

* Zero risk to existing consumers. No metric semantics change.
* Internal consumers (rejection, backpressure) already need separate work to handle native memory — they will consume the new `native_*` metrics directly rather than relying on summed aggregates.
* External consumers see new metrics appear but their existing dashboards/alarms continue to work unchanged.
* Consumers who want a combined view can sum `primary_in_bytes + native_primary_in_bytes` on their side.


## 4. Node Stats API `_nodes/stats`

### 4.1 What's New

| Stats | Description | New Metrics |
|---|---|---|
| `native_tasks` | Per-task-type stats for native (Tokio) async operations. Each JNI operation type gets its own named section with task counters (queued/active/completed) and `tokio_metrics::TaskMonitor` health metrics. Modeled after JVM `thread_pool` — per-workload-type breakdown for diagnosing which native operation is under pressure. | • `native_tasks.{task_type}` — one section per JNI operation type (`query_execution`, `stream_next`, `fetch_phase`, `segment_stats`), each containing:<br><br>Task counters (application-level `AtomicUsize`/`AtomicU64`):<br>&nbsp;&nbsp;• `queued` — tasks waiting to be polled (point-in-time gauge)<br>&nbsp;&nbsp;• `active` — tasks currently executing, from first poll to completion (point-in-time gauge)<br>&nbsp;&nbsp;• `completed` — total tasks finished (cumulative counter)<br><br>TaskMonitor metrics (from `tokio_metrics::TaskMonitor`, cumulative):<br>&nbsp;&nbsp;• `total_poll_duration_ms` — time spent actually executing<br>&nbsp;&nbsp;• `total_scheduled_duration_ms` — time waiting in queue before being polled<br>&nbsp;&nbsp;• `total_idle_duration_ms` — time between polls waiting on external dependency (IO, timer)<br>&nbsp;&nbsp;• `total_slow_poll_count` — polls exceeding threshold (100µs for query/fetch/segment, 50µs for stream)<br>&nbsp;&nbsp;• `total_long_delay_count` — tasks waiting >50ms before first poll (runtime overloaded)<br>&nbsp;&nbsp;• `slow_poll_ratio` — fraction of polls that were slow |

### 4.2 What's Changed

| Stats | What Changes | New Metrics | Existing Metrics Impact |
|---|---|---|---|
| `os` | TBD — awaiting PoC | TBD | TBD |
| `process` | TBD — awaiting PoC | TBD | TBD |
| `indexing_pressure` | Native write buffer memory | • `native_primary_in_bytes` — native primary memory<br>• `native_replica_in_bytes` — native replica memory<br>• `native_all_in_bytes` — native total memory<br>(all under both `current` and `total`) | • `primary_in_bytes` — unchanged, remains JVM-only<br>• `replica_in_bytes` — unchanged, remains JVM-only<br>• `all_in_bytes` — unchanged, remains JVM-only<br>• `limit_in_bytes` — unchanged, remains JVM-heap-derived (10% of heap)<br><br>Rejection path stays JVM-only: `IndexingPressure` checks JVM bytes against the JVM-heap-derived limit. Native memory is off-heap — checking it against a JVM limit doesn't make physical sense. Native memory is enforced separately via child circuit breakers (e.g., `native_write_buffer` in the `breaker` section). |
| `shard_indexing_pressure` | Native memory per shard | • `native_current_in_bytes` — native current memory for this shard<br>• `native_total_in_bytes` — native cumulative memory for this shard | • `current_in_bytes` — unchanged, remains JVM-only<br>• `total_in_bytes` — unchanged, remains JVM-only<br>Existing per-shard rejection logic continues to use JVM-only values. |
| `search_backpressure` | Native query resource tracking | TBD as per backpressure design | TBD as per backpressure design |
| `resource_usage_stats` | Native resource usage added | • `native_cpu_utilization_percent` — native runtime CPU usage<br>• `native_memory_utilization_percent` — native runtime memory usage | • `cpu_utilization_percent` — remains JVM-only (no summing)<br>• `memory_utilization_percent` — remains JVM-only (no summing)<br>Native resource usage reported separately, not mixed into JVM aggregates. Per-task-type breakdown available via the new `native_tasks` stat (see §4.1). |
| `refresh` | Parquet flush during refresh | None — no new fields | • `total_time_in_millis` — now implicitly includes Parquet flush time (CompositeEngine refresh triggers both Lucene refresh and Parquet flush)<br>• No summing — single timer covers both |
| `breaker` | Native child breakers auto-registered | • `native_write_buffer` — Parquet write buffer breaker (auto-appears)<br>• `native_query_memory` — DataFusion query memory breaker (auto-appears)<br>Registered via `CircuitBreakerPlugin.getCircuitBreaker()` | • No changes to parent breaker or existing child breakers<br>• Parent checks real JVM heap via `MemoryMXBean`, doesn't include native off-heap |
| `caches` | DataFusion native cache stats | • TBD — depends on approach chosen | • No changes to existing `INDICES_REQUEST_CACHE`<br>• Two options (decision deferred):<br>&nbsp;&nbsp;• **Option A**: New `CacheType` enum + `ICache` JNI wrapper → auto-iterated by `CacheService`<br>&nbsp;&nbsp;• **Option B**: Expose via native metrics SPI under a different stats section |
| `repositories` | Parquet file snapshot operations | • TBD — new metrics for Parquet file snapshot/restore operations (e.g., native file upload counts, native file sizes, native snapshot durations) | • Existing Lucene segment snapshot metrics unchanged<br>• Parquet files are a new artifact type that needs to be snapshotted alongside Lucene segments — new metrics track the native file operations separately |
| `file_cache` | Writable warm (separate discussion) | • TBD | • TBD — file cache currently read-only<br>• Writable warm (Parquet files in cache) is a separate design discussion |

### 4.3 What Remains Unchanged

| Stats | Reason |
|---|---|
| `jvm` | JVM heap, GC, threads are JVM-only. Native Rust memory is off-heap and not tracked by JVM stats. |
| `thread_pool` | JVM thread pools unchanged. Parquet writes use existing `WRITE` pool, refresh uses `REFRESH` pool. DataFusion Tokio threads are invisible to JVM thread pools — native async task stats are exposed via `native_tasks` (see §4.1). |
| `fs` | Filesystem stats (total/free/available) are OS-level. Parquet files on disk are visible to existing `FsProbe`. |
| `transport` | JVM transport layer unchanged. |
| `http` | HTTP server connections unchanged. Native components don't add HTTP endpoints. |
| `script` | Script compilation/caching is JVM-only. No native script execution. |
| `discovery` | Discovery is entirely JVM-managed. Cluster state, published states, pending tasks — all JVM. Does NOT change. |
| `adaptive_selection` | Node selection for search routing is JVM-side. Response times may change with DataFusion but the stats structure doesn't. |
| `script_cache` | Same as `script` — JVM-only. |
| `cluster_manager_throttling` | Cluster manager task throttling is JVM-only. |
| `weighted_routing` | Weighted routing fail-open stats are JVM-only. |
| `search_pipeline` | Search pipeline processors run on JVM. DataFusion is downstream of search pipelines. |
| `task_cancellation` | All cancellations happen on JVM. Native blindly executes. Stats reflect JVM-side cancellation decisions. |
| `segment_replication_backpressure` | Segment replication is Lucene-segment-based. Parquet files have separate replication (future work). |
| `admission_control` | Admission control decisions happen on JVM. Native resource usage may inform decisions but the stats structure doesn't change. |
| `remote_store_node_stats` | Remote store upload/download stats are JVM-managed. |
| `ingest` | Ingest pipelines run entirely on JVM before documents reach the engine. Confirmed unchanged. |

---

## 5. Index Stats API `{index}/_stats`

### 5.1 What's New

_Content TBD — placeholder for new index-level metrics._

### 5.2 What's Changed

| Stats | What Changes | Details |
|---|---|---|
| `store` | New `native_store` parent field | Parquet files contribute to shard storage. Rather than adding sub-fields to existing `store`, a new `native_store` parent field exposes: `size_in_bytes` (total Parquet file size), `file_count`, `row_group_count`. Existing `store.size_in_bytes` continues to reflect Lucene-only size for backward compatibility. |
| `segments` | New `native_segments` parent field | Parquet "segments" (row groups) are analogous to Lucene segments. New `native_segments` parent field exposes: `count` (number of Parquet row groups), `memory_in_bytes` (native memory for open readers), `file_count`. Existing `segments` continues to reflect Lucene-only segments. |

### 5.3 What Remains Unchanged

The following 12 index stats flags are unchanged by native plugins:

| Stats | Reason |
|---|---|
| `docs` | Document counts come from Lucene. Parquet row counts are available via `CatalogSnapshot` but are not yet exposed (future work). |
| `indexing` | Indexing stats (count, time, throttle) are collected at the JVM `IndexShard` level, upstream of the engine. Both Lucene and Parquet writes are captured. |
| `get` | Get-by-ID operations go through Lucene only. DataFusion handles range queries/aggregations, not point lookups. |
| `search` | Search stats (query/fetch counts, times) are collected at the JVM level. DataFusion execution time is included in overall query time. |
| `merge` | Merge stats are Lucene-specific. Parquet compaction/merge is a separate operation (see Open Question 8). |
| `refresh` | Refresh stats are collected at JVM level. Parquet flush time is included in refresh time (see Node Stats §4.2). |
| `flush` | Flush stats are Lucene translog flush. Parquet flush is tied to refresh, not translog flush. |
| `warmer` | Index warming is Lucene-specific. |
| `query_cache` | Query cache is JVM-managed. DataFusion has its own caching (not yet exposed). |
| `fielddata` | Fielddata is JVM heap. Native memory tracked separately. |
| `completion` | Completion suggester is Lucene-only. |
| `translog` | Translog is Lucene-specific. Parquet has its own durability model. |
| `request_cache` | Request cache is JVM-managed. |
| `recovery` | Recovery stats are Lucene-segment-based. Parquet file recovery is separate (future work). |

---

## 6. Cluster Stats API `_cluster/stats`

### 6.1 What's New

_Content TBD — placeholder for new cluster-level metrics._

### 6.2 What's Changed

| Section | What Changes | Details |
|---|---|---|
| `indices.store` | Aggregates `native_store` across cluster | `ClusterStatsIndices` aggregates per-shard `StoreStats`. With `native_store` added at shard level, cluster stats will include aggregated native store size across all shards/indices. |
| `indices.segments` | Aggregates `native_segments` across cluster | Same aggregation pattern. Cluster-wide native segment (Parquet row group) counts and memory. |

### 6.3 What Remains Unchanged

No new cluster-level node metrics are needed. The existing 10 node-level sections (`os`, `process`, `jvm`, `fs`, `plugins`, `network_types`, `discovery_types`, `packaging_types`, `ingest`, `node_count`) are unchanged. Native plugin information will appear in the `plugins` section automatically via standard plugin registration.

The remaining 7 index-level sections (`count`, `shards`, `docs`, `fielddata`, `query_cache`, `completion`, `mappings`) are unchanged — they aggregate the same JVM-level stats that don't change with native plugins.

---

## 7. Open Questions

| Question | Status | Notes |
|---|---|---|
| How to handle backward compatibility when adding native fields to `indexing_pressure`? | Resolved | Option A: add `native_*` fields alongside existing JVM-only fields. Existing fields unchanged — no summing, no semantic change. Rejection path stays JVM-only. Native memory enforced separately via child circuit breakers. |
| Should per-worker stats be opt-in (query parameter) or always returned? | Open | Per-worker breakdown can be verbose. Consider `?include_workers=true` or similar. |
| How does `SearchBackpressureService` get native resource usage for cancellation decisions? | Resolved | Via `ServiceCache` pattern. Native feeds resource usage into `ResourceUsageCollectorService`. |
| Should Parquet merge/compaction stats be exposed via `merge` or a new field? | Open | Lucene merge and Parquet compaction are different operations. Mixing them in `merge` could confuse users. |
| How to attribute native memory to specific shards for `shard_indexing_pressure`? | Open | Write module needs shard-level memory tracking. Currently native memory is process-wide. |
| Should `CatalogSnapshot` metadata (row counts, column stats) be exposed in stats APIs? | Open | Useful for debugging but potentially large. Could be a separate endpoint. |
| How to handle `file_cache` for writable warm tier with Parquet files? | Open | Separate design discussion. File cache is currently read-only. |
| How to expose Parquet merge/compaction stats at the index level? | Open | Related to OQ4. Need to decide on field naming and structure. |
| Should `native_store` and `native_segments` be included in `_cat/indices` output? | Open | `_cat` APIs derive from index stats. If new parent fields are added, `_cat` may need new columns. |
| What SPI/interface should native plugins use to report metrics to `_nodes/stats`? | Open | No instrumentation bridge exists today. Need to design a metrics collection SPI for native plugins. |

---

## 8. Appendix

### Appendix A: Summary Matrix

#### Node Stats Summary

| Stats | Impact | Change Type | Notes |
|---|---|---|---|
| `native_tasks` | High | New | Per-task-type stats for native async operations (queued/active/completed counters + TaskMonitor health metrics per JNI operation type: query_execution, stream_next, fetch_phase, segment_stats) |
| `indexing_pressure` | High | Modified | Native write buffer memory, new `native_*` fields alongside unchanged JVM-only fields |
| `shard_indexing_pressure` | High | Modified | Per-shard native memory attribution, new `native_*` fields alongside unchanged JVM-only fields |
| `os` | TBD | TBD | Awaiting PoC |
| `process` | TBD | TBD | Awaiting PoC |
| `search_backpressure` | Medium | Modified | Native query resource tracking |
| `resource_usage_stats` | Medium | Modified | Native resource usage via ServiceCache |
| `refresh` | Medium | Modified | Includes Parquet flush time |
| `breaker` | Medium | Modified | Native child breakers registered via `CircuitBreakerPlugin`; auto-appear in stats |
| `caches` | Medium | Modified | DataFusion native cache stats; two options under evaluation (see §4.2) |
| `file_cache` | Low | Future | Writable warm — separate discussion |
| `repositories` | Medium | Modified | Parquet file snapshot/restore operations — new metrics for native file uploads, sizes, durations |
| `ingest` | None | Unchanged | Confirmed: runs before engine |
| 15 other metrics | None | Unchanged | JVM-only, no native component involvement |

#### Index Stats Summary

| Stats | Impact | Change Type | Notes |
|---|---|---|---|
| `store` | High | New parent field | `native_store` for Parquet file stats |
| `segments` | High | New parent field | `native_segments` for Parquet row groups |
| `docs` | Low | Future | Parquet row counts via CatalogSnapshot |
| `merge` | Low | Future | Parquet compaction stats (see OQ5/OQ8) |
| 12 other flags | None | Unchanged | JVM-level collection, no native changes |

#### Cluster Stats Summary

| Section | Impact | Change Type | Notes |
|---|---|---|---|
| `indices.store` | High | Modified | Aggregates `native_store` across cluster |
| `indices.segments` | High | Modified | Aggregates `native_segments` across cluster |
| All other sections | None | Unchanged | JVM-level aggregation, no native changes |

### Appendix B: Example API Responses

#### B.1 `indexing_pressure` example

```json
{
  "indexing_pressure": {
    "memory": {
      "current": {
        "coordinating_in_bytes": 0,
        "primary_in_bytes": 524288000,
        "replica_in_bytes": 0,
        "all_in_bytes": 524288000,
        "native_primary_in_bytes": 67108864,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 67108864
      },
      "total": {
        "coordinating_in_bytes": 0,
        "primary_in_bytes": 10485760000,
        "replica_in_bytes": 0,
        "all_in_bytes": 10485760000,
        "native_primary_in_bytes": 1073741824,
        "native_replica_in_bytes": 0,
        "native_all_in_bytes": 1073741824
      },
      "limit_in_bytes": 1073741824
    }
  }
}
```

Existing fields (`primary_in_bytes`, etc.) remain JVM-only. New `native_*` fields expose native memory alongside them. Consumers who want a combined view sum `primary_in_bytes + native_primary_in_bytes` on their side.

#### B.2 `resource_usage_stats` example

```json
{
  "resource_usage_stats": {
    "node_id_1": {
      "timestamp": 1700000000000,
      "cpu_utilization_percent": "0.45",
      "memory_utilization_percent": "0.62",
      "native_cpu_utilization_percent": "0.23",
      "native_memory_utilization_percent": "0.15"
    }
  }
}
```

#### B.3 `native_tasks` example

```json
{
  "native_tasks": {
    "query_execution": {
      "queued": 5,
      "active": 3,
      "completed": 12840,
      "total_poll_duration_ms": 892000,
      "total_scheduled_duration_ms": 4200,
      "total_idle_duration_ms": 15600,
      "total_slow_poll_count": 42,
      "total_long_delay_count": 3,
      "slow_poll_ratio": 0.003
    },
    "stream_next": {
      "queued": 12,
      "active": 8,
      "completed": 51200,
      "total_poll_duration_ms": 1230000,
      "total_scheduled_duration_ms": 8900,
      "total_idle_duration_ms": 340000,
      "total_slow_poll_count": 18,
      "total_long_delay_count": 1,
      "slow_poll_ratio": 0.0004
    },
    "fetch_phase": {
      "queued": 0,
      "active": 1,
      "completed": 6420,
      "total_poll_duration_ms": 128000,
      "total_scheduled_duration_ms": 1100,
      "total_idle_duration_ms": 89000,
      "total_slow_poll_count": 7,
      "total_long_delay_count": 0,
      "slow_poll_ratio": 0.001
    },
    "segment_stats": {
      "queued": 0,
      "active": 0,
      "completed": 320,
      "total_poll_duration_ms": 4800,
      "total_scheduled_duration_ms": 200,
      "total_idle_duration_ms": 12000,
      "total_slow_poll_count": 0,
      "total_long_delay_count": 0,
      "slow_poll_ratio": 0.0
    }
  }
}
```

Task counters (`queued`, `active`, `completed`) are application-level `AtomicUsize`/`AtomicU64` counters maintained per JNI operation type. TaskMonitor metrics are from `tokio_metrics::TaskMonitor` — each JNI function wraps its async work with `.instrument()` to collect cumulative timing and slow-poll data. Together they provide the native equivalent of JVM `thread_pool` per-pool stats: "which operation type is under pressure, how much work is queued, and how is it performing."

### Appendix C: Integration Points (Cross-Cutting Concerns)

#### C.1 Native metrics collection SPI (TBD)

No instrumentation bridge exists today between native (Rust) runtimes and `_nodes/stats`. A metrics collection SPI needs to be designed that supports:
- Multi-plugin registration (DataFusion + Parquet reporting independently)
- Per-task-type breakdown (queued/active/completed counters + TaskMonitor health metrics per JNI operation)
- Structured return types (not just flat `double[]` arrays)
- JNI-based collection (JVM pulls from Rust on demand)

#### C.2 Per-task-type stats schema

Each JNI operation type (`query_execution`, `stream_next`, `fetch_phase`, `segment_stats`) gets its own named section with:
- Application-level `AtomicUsize`/`AtomicU64` counters for queued/active/completed — maintained on the Rust side, incremented/decremented around each async dispatch
- `tokio_metrics::TaskMonitor` cumulative metrics — each JNI function wraps its async work with `.instrument()` to collect poll duration, scheduling delay, idle time, and slow-poll counts
- These are already partially implemented: `TaskMonitor` instances exist per JNI function in `lib.rs`, and `NativeTaskStats` on the Java side structures them as named groups. The missing piece is the `AtomicUsize` task counters (queued/active/completed) which need to be added around each `io_runtime.block_on()` call.

#### C.3 ServiceCache pattern

Native resource usage feeds into `ResourceUsageCollectorService` via the `ServiceCache` pattern:

- Rust side collects per-worker CPU/memory stats
- JNI bridge exposes these as `ServiceCache` entries
- `ResourceUsageCollectorService` reads from `ServiceCache` on its collection interval
- Stats appear in `resource_usage_stats` (aggregated with JVM stats)

This ensures native resource usage is visible through the existing resource usage infrastructure.

#### C.4 Write module shard-level memory attribution

The Parquet write module maintains native memory (Arrow RecordBatches) that needs to be attributed to specific shards for `shard_indexing_pressure`. Current challenge: native memory is allocated process-wide by the Rust allocator. Shard-level tracking requires the write module to maintain per-shard accounting on the Rust side and expose it via JNI.

#### C.5 Parquet merge stats exposure

Parquet files undergo compaction (merging small files into larger ones). This is analogous to but distinct from Lucene segment merges. Stats to expose: files merged, bytes before/after, compaction time, pending compactions. Decision needed on whether to use existing `merge` field or a new `native_merge`/`parquet_compaction` field (see Open Question 5 and Open Question 8).
