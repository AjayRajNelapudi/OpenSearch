# Mustang: Plugin Stats for Node Stats API

## 1. Introduction

This document describes the design for exposing per-plugin operational metrics through OpenSearch's `_nodes/stats` API. Project Mustang runs query execution on Rust/Tokio runtimes loaded as shared libraries via JNI. Each plugin produces its own operational metrics (runtime thread stats, task monitors, queue depths, etc.) that today have no path into the stats API.

The goal is to define an SPI contract that lets each Rust-based plugin emit its stats to `NodeStats`, using protobuf-encoded `byte[]` over JNI. The SPI is intentionally minimal — it does not prescribe how plugins structure their metrics internally, and individual plugins can later extend the same mechanism to feed other consumers (admission control, search backpressure, external monitoring). Those integrations are out of scope for this design.

Scope:

* Expose per-plugin stats in `_nodes/stats` and `_cluster/stats` responses
* Define `PluginStats` SPI interface and `MetricProvider` contract
* Protobuf as the JNI wire format, with each plugin owning its `.proto` schema
* Cached reads via `ServiceCache` (same pattern as `OsService`, `JvmService`)

Out of scope:

* Admission control, search backpressure, or threadpool rejection integration
* Unified metric collection layer across all OpenSearch consumers
* Per-index or per-shard metric granularity
* Metric filtering or aggregation across plugins


## 2. Current State

The stats API path uses MonitorService's lazy cache pattern: `OsService`, `ProcessService`, `JvmService`, and `FsService` each wrap their probe behind a `SingleObjectCache` with a 1-second TTL. No background threads — the cache refreshes lazily when a caller reads and the TTL has expired. This serves the node stats and cluster stats REST APIs.

Other consumers (admission control, search backpressure, BeagleStone) each have their own collection strategies — active background polling, direct probe reads, periodic external API calls. These are documented here for context but are not in scope for this design. The SPI contract defined here does not preclude later integration with these consumers, but this document focuses solely on the stats API path.


## 3. Problem Statement

### 3.1 Plugin-based Rust execution

Project Mustang introduces Rust execution engines as OpenSearch plugins. DataFusion and ParquetWriter are the first, with more expected.

Each plugin:

* Loads its own shared library via `System.loadLibrary` in its plugin classloader
* Manages Tokio runtimes and native resources
* Produces operational metrics (worker thread counts, queue depths, task durations) that are not visible to OpenSearch core

Today, the stats API has no way to surface these metrics. There is no SPI contract for a plugin to expose its stats to `NodeStats`.

### 3.2 No plugin stats path in the stats API

The `_nodes/stats` API surfaces JVM, OS, process, filesystem, and index-level stats — all from Java-native sources. There is no mechanism for a Rust-based plugin to contribute its own stats section to the response. Each plugin would need to define ad-hoc JNI entry points, its own serialization format, and wire itself into `NodeStats` and `NodeService` without any shared contract.

### 3.3 Classloader constraints

`System.loadLibrary` binds the shared library to the classloader that called it. The SPI jar is loaded by the system classloader, but each plugin's shared library is loaded by the plugin classloader. A static method declared in the SPI jar cannot resolve JNI symbols from a plugin's shared library — this produces `UnsatisfiedLinkError` at runtime.

**Implication**: The JNI entry point for metric collection must live in each plugin's classloader, not in the shared SPI module.


## 4. Requirements

* **Plugin stats in the stats API** — Each Rust-based plugin can expose its operational metrics as a named section in the `_nodes/stats` response, alongside existing sections like `os`, `jvm`, `process`.
* **SPI contract for stats emission** — A minimal interface (`MetricProvider` + `PluginStats`) in the SPI module that any plugin can implement to emit stats. The contract enforces `Writeable` (transport serialization) and `ToXContentFragment` (REST JSON rendering).
* **Protobuf over JNI** — Metrics cross the JNI boundary as protobuf-encoded `byte[]`. Each plugin defines its own `.proto` schema. The SPI does not prescribe the schema — plugins own their metric structure.
* **Cached reads** — Stats API reads from a `ServiceCache` with a configurable TTL, same pattern as `OsService`/`JvmService`. No background threads — lazy refresh on read.
* **Schema evolution** — New metric fields are additive and backward-compatible. Adding a new plugin requires no changes to the SPI or server framework code.
* **Graceful lifecycle** — Metric collection starts and stops cleanly with the plugin lifecycle. No JVM hangs, thread leaks, or crashes on shutdown.
* **Extensible for future consumers** — The SPI should not preclude later integration with admission control, search backpressure, or external monitoring. But those integrations are not in scope.


## 5. Approaches

### 5.1 Pull: Single stats() JNI call, PluginStats SPI, Java-side Provider and Cache

#### Key Design Principles

* **Simple Rust Side** — Rust exposes a single bare extern "C" JNI function (`stats()`) returning all metrics as protobuf-encoded `byte[]`. Each plugin defines its own `.proto` schema and decides what goes in the payload.
* **MetricProvider is minimal** — Single-method interface in the SPI: `PluginStats stats()`. No per-category methods. No individual metric accessors. The provider is not coupled to any specific plugin's internal structure.
* **PluginStats in SPI, concrete stats in plugin** — `PluginStats` (SPI) extends `NamedWriteable + ToXContentFragment`. Each plugin defines its own concrete stats class (e.g. `DataFusionPluginStats`) that implements `PluginStats`. The plugin owns its schema entirely.
* **ServiceCache\<PluginStats\> replaces Probe + Service** — Generic class: `SingleObjectCache<PluginStats>` + `Supplier<PluginStats>` lambda. NodeService holds one per plugin, wrapping provider calls. No per-category Service or Probe subclasses needed.
* **NodeService owns the ServiceCache instances** — Same pattern as OsService/JvmService but without per-category Service or Probe subclasses. NodeService calls `serviceCache.getOrRefresh()` which calls the provider on cache miss.
* **Transport serialization via NamedWriteableRegistry** — Each plugin registers its concrete stats class with a name (e.g. `"datafusion"`). `NodeStats` reads/writes via `readOptionalNamedWriteable(PluginStats.class)` / `writeOptionalNamedWriteable()`. The server never imports the concrete plugin stats class.

#### New Interfaces and Classes

**SPI (`libs/vectorized-exec-spi`)**

```java
/**
 * Interface for plugin-provided stats.
 * Each plugin defines its own concrete implementation.
 *
 */
public interface PluginStats extends NamedWriteable, ToXContentFragment {

    /**
     * Returns the name used to identify this stats class in the
     * NamedWriteableRegistry (e.g. "datafusion").
     * Inherited from NamedWriteable.
     */
    @Override
    String getWriteableName();

    /**
     * Serialize this stats object to the transport stream.
     * Inherited from Writeable (via NamedWriteable).
     */
    @Override
    void writeTo(StreamOutput out) throws IOException;

    /**
     * Render this stats object as JSON for the REST API.
     * Inherited from ToXContentFragment.
     */
    @Override
    XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;
}
```

```java
/**
 * Single-method interface for collecting metrics from a plugin.
 * Lives in SPI; implemented by each plugin in its own classloader.
 */
public interface MetricProvider {
    PluginStats stats();
}
```

**Server (`server/`)**

```java
/**
 * Generic cache backed by SingleObjectCache that refreshes
 * from a Supplier<S> when the configured TTL expires.
 * Replaces the per-category Probe + Service pattern.
 */
public class ServiceCache<S> {
    private final SingleObjectCache<S> cache;

    public ServiceCache(Supplier<S> supplier, TimeValue refreshInterval) { ... }
    public S getOrRefresh() { return cache.getOrRefresh(); }
}
```

`NodeStats` — adds `@Nullable PluginStats dataFusionPluginStats` field. Serialized via `readOptionalNamedWriteable(PluginStats.class)` / `writeOptionalNamedWriteable()`.

`NodeService` — holds `@Nullable ServiceCache<PluginStats> dataFusionService`. Calls `dataFusionService.getOrRefresh()` when `nativeMetrics=true`.

**Plugin (`plugins/engine-datafusion`)**

```java
/**
 * Concrete stats for the DataFusion plugin.
 * Owns its schema entirely — all runtime + task monitor metrics.
 * Registered with NamedWriteableRegistry under name "datafusion".
 */
public class DataFusionPluginStats implements PluginStats {
    // Fields decoded from protobuf byte[] returned by JNI
    // Implements writeTo(), readFrom(), toXContent()
    @Override
    String getWriteableName() {

    }

    /**
     * Serialize this stats object to the transport stream.
     * Inherited from Writeable (via NamedWriteable).
     */
    @Override
    void writeTo(StreamOutput out) throws IOException {

    }

    /**
     * Render this stats object as JSON for the REST API.
     * Inherited from ToXContentFragment.
     */
    @Override
    XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

    }
}
```

```java
/**
 * MetricProvider for DataFusion. Lives in plugin classloader
 * (required: JNI symbols bound to plugin classloader).
 * Calls NativeBridge.stats() → byte[] → DataFusionPluginStats.decode().
 */
public class DataFusionMetricProvider implements MetricProvider {
    @Override
    public PluginStats stats() {
        byte[] bytes = NativeBridge.stats();
        return DataFusionPluginStats.decode(bytes);
    }
}
```

**Rust (`plugins/engine-datafusion/jni/`)**

```rust
// Single bare JNI function. No registry, no provider trait, no filter.
// Returns protobuf-encoded byte[] with all runtime + task monitor metrics.
#[no_mangle]
pub extern "C" fn Java_...NativeBridge_stats(env: JNIEnv, _class: JClass) -> jbyteArray {
    let stats = collect_all_metrics(); // collect metrics as plugin requries
    let bytes = stats.encode_to_vec(); // prost
    env.byte_array_from_slice(&bytes).unwrap()
}
```

Each plugin also defines its own `.proto` schema (e.g. `datafusion_stats.proto`) used by both prost (Rust) and protoc (Java).

#### Execution Flow: Stats API

```
GET /_nodes/stats/native_metrics
    → TransportNodesStatsAction
    → NodeService.stats(nativeMetrics=true)
    → dataFusionService.getOrRefresh()          // ServiceCache<PluginStats>
        → DataFusionMetricProvider.stats()
            → DataFusionPluginStats.decode(byte[])
    → new NodeStats(..., dataFusionPluginStats)
```

#### Runtime Environment

```
+=====================================================================+
|                               JVM PROCESS                           |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  server/ (OpenSearch core)                                    │  |
|  │                                                               │  |
|  │  NodeService                                                  │  |
|  │    @Nullable ServiceCache<PluginStats> dataFusionService      │  |
|  │                                                               │  |
|  │  NodeStats                                                    │  |
|  │    @Nullable PluginStats dataFusionPluginStats                │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  libs/vectorized-exec-spi (Java SPI)                          │  |
|  │                                                               │  |
|  │  MetricProvider (interface)                                   │  |
|  │    └── stats() → PluginStats                                  │  |
|  │                                                               │  |
|  │  PluginStats (interface)                                      │  |
|  │    extends NamedWriteable, ToXContentFragment                 │  |
|  │                                                               │  |
|  │  ServiceCache<S>                                              │  |
|  │    SingleObjectCache<S> + Supplier<S> lambda                  │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  plugins/engine-datafusion (Java)                             │  |
|  │                                                               │  |
|  │  DataFusionMetricProvider implements MetricProvider           │  |
|  │  DataFusionPluginStats implements PluginStats                 │  |
|  │                                                               │  |
|  └──────────────────────────────┬────────────────────────────────┘  |
|                                 │ JNI                               |
|  ┌──────────────────────────────▼────────────────────────────────┐  |
|  │  plugins/engine-datafusion (Rust cdylib)                      │  |
|  │                                                               │  |
|  │  stats() → byte[] (protobuf-encoded)                          │  |
|  │    prost encodes all runtime + task monitor metrics           │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
+=====================================================================+
```

#### Pros

* **Simplest Rust side** — Single bare JNI function per plugin, no registry, no provider trait, no filtering logic
* **Low-overhead JNI crossing** — byte[] crosses JNI as a single allocation + memcpy, cheaper than strings or objects. Precedent: Substrait plans already cross JNI as byte[] in this plugin.
* **Supports nested structures** — Protobuf naturally represents nested metric pools, variable-length lists, and optional fields. Schema evolution is built in (additive fields are backward-compatible).
* **No Probe or Service boilerplate** — ServiceCache\<S\> + Supplier\<S\> lambda replaces both
* **Plugin owns its schema** — Each plugin defines its own `.proto` and stats class. The SPI only defines the contract (PluginStats). Adding new metrics to a plugin requires no SPI or server changes.
* **Minimal SPI surface** — MetricProvider has one method. PluginStats is a marker interface. No plugin-specific types leak into the SPI.
* **Independent caching** — Each ServiceCache has its own TTL, NodeService owns the caches
* **Follows OpenSearch patterns** — SingleObjectCache is the same mechanism used by OsService, ProcessService. NamedWriteableRegistry is the standard pattern for polymorphic transport serialization.
* **Classloader safe** — Provider is a Java object registered by the plugin from its own classloader. NamedWriteableRegistry handles deserialization dispatch without compile-time coupling.

#### Cons

* **One Stats class + one .proto per plugin** — Each new plugin needs a Stats class and a proto definition. But this is the domain model, not boilerplate — fields are genuinely different per plugin.
* **Proto dependency** — Both Rust (prost) and Java (protoc) sides depend on the same `.proto` file. Schema changes require regenerating code on both sides.
* **NamedWriteableRegistry ceremony** — Each plugin must register its stats class. A few lines of code per plugin.


### 5.2 Pull: Concrete Stats and Proto in SPI (Variant of 5.1, Recommended)

This approach builds on 5.1 but moves the concrete stats class (`DataFusionPluginStats`) and its `.proto` schema from the plugin jar into `libs/vectorized-exec-spi`. Since the SPI module is on the server's classpath, server-side consumers get typed field access directly — no downcasting, no adapters, no `NamedWriteableRegistry` ceremony.

#### Motivation

In 5.1, `NodeStats` holds an opaque `PluginStats` interface reference. When a server-side consumer (e.g., thread pool stats, admission control) needs to read specific metric pools from the plugin stats (e.g., Tokio worker counts for `thread_pool`), it cannot access the concrete fields without downcasting — the server doesn't have the plugin jar on its classpath.

Moving `DataFusionPluginStats` to the SPI solves this: the server knows the concrete type at compile time, just like it knows `OsStats` and `JvmStats`.

#### Key Differences from 5.1

* `DataFusionPluginStats` lives in `libs/vectorized-exec-spi` instead of `plugins/engine-datafusion`
* `datafusion_stats.proto` lives in `libs/vectorized-exec-spi` (Java protoc codegen in SPI's `build.gradle`, Rust prost codegen in SPI's `Cargo.toml`)
* `PluginStats` interface is retained as the contract for `MetricProvider.stats()` — enforces `Writeable + ToXContentFragment`. But it drops `NamedWriteable` since the server knows the concrete type.
* `NodeStats` holds `@Nullable DataFusionPluginStats` directly (not `PluginStats`), uses plain `readOptionalWriteable` / `writeOptionalWriteable` — same pattern as `OsStats`, `JvmStats`
* No `NamedWriteableRegistry` entry needed
* `ServiceCache<DataFusionPluginStats>` instead of `ServiceCache<PluginStats>`

#### New Interfaces and Classes

**SPI (`libs/vectorized-exec-spi`)**

```java
/**
 * Contract for plugin-provided stats.
 * Enforces Writeable + ToXContentFragment on any stats object
 * returned by MetricProvider.stats().
 *
 * Unlike 5.1, this does NOT extend NamedWriteable — the server
 * knows the concrete type (DataFusionPluginStats) at compile time.
 */
public interface PluginStats extends Writeable, ToXContentFragment {
}
```

```java
/**
 * Single-method interface for collecting metrics from a plugin.
 * Lives in SPI; implemented by each plugin in its own classloader.
 */
public interface MetricProvider {
    PluginStats stats();
}
```

```java
/**
 * Concrete stats for the DataFusion plugin.
 * Lives in SPI so server consumers get typed field access.
 * Contains all runtime (IO + CPU) and task monitor metrics.
 */
public class DataFusionPluginStats implements PluginStats {
    @Nullable RuntimeValues ioRuntime;
    @Nullable RuntimeValues cpuRuntime;
    TaskMonitorValues queryExecution;
    TaskMonitorValues streamNext;
    TaskMonitorValues fetchPhase;
    TaskMonitorValues segmentStats;

    public static DataFusionPluginStats decode(byte[] bytes) { ... }
    // writeTo(), StreamInput constructor, toXContent(), getters
}
```

**Plugin (`plugins/engine-datafusion`)**

```java
/**
 * MetricProvider for DataFusion. Lives in plugin classloader
 * (required: JNI symbols bound to plugin classloader).
 * Calls NativeBridge.stats() → byte[] → DataFusionPluginStats.decode().
 */
public class DataFusionMetricProvider implements MetricProvider {
    @Override
    public PluginStats stats() {
        byte[] bytes = NativeBridge.stats();
        return DataFusionPluginStats.decode(bytes);
    }
}
```

**Server (`server/`)**

```java
// NodeStats — typed field, plain Writeable (same pattern as OsStats)
@Nullable DataFusionPluginStats dataFusionPluginStats;

// Serialization:
out.writeOptionalWriteable(dataFusionPluginStats);

// Deserialization:
dataFusionPluginStats = in.readOptionalWriteable(DataFusionPluginStats::new);
```

**Rust** — Identical to 5.1. Single `stats()` JNI function returning protobuf `byte[]`.

#### Runtime Environment

```
+=====================================================================+
|                               JVM PROCESS                           |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  server/ (OpenSearch core)                                    │  |
|  │                                                               │  |
|  │  NodeService                                                  │  |
|  │    @Nullable ServiceCache<DataFusionPluginStats>              │  |
|  │                                                               │  |
|  │  NodeStats                                                    │  |
|  │    @Nullable DataFusionPluginStats dataFusionPluginStats      │  |
|  │    (typed — same pattern as OsStats, JvmStats)                │  |
|  │                                                               │  |
|  │  Other consumers (typed access):                              │  |
|  │    ThreadPool stats → dataFusionPluginStats.getIoRuntime()    │  |
|  │    AdmissionControl → dataFusionPluginStats.getIoRuntime()    │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  libs/vectorized-exec-spi (Java SPI)                          │  |
|  │                                                               │  |
|  │  MetricProvider (interface)                                   │  |
|  │    └── stats() → PluginStats                                  │  |
|  │                                                               │  |
|  │  PluginStats (interface)                                      │  |
|  │    extends Writeable, ToXContentFragment                      │  |
|  │                                                               │  |
|  │  DataFusionPluginStats implements PluginStats  ← MOVED HERE  │  |
|  │    .proto + protoc codegen also in SPI                        │  |
|  │                                                               │  |
|  │  ServiceCache<S>                                              │  |
|  │    SingleObjectCache<S> + Supplier<S> lambda                  │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
|                                                                     |
|  ┌───────────────────────────────────────────────────────────────┐  |
|  │  plugins/engine-datafusion (Java)                             │  |
|  │                                                               │  |
|  │  DataFusionMetricProvider implements MetricProvider           │  |
|  │    stats() → NativeBridge.stats() → decode(byte[])           │  |
|  │                                                               │  |
|  └──────────────────────────────┬────────────────────────────────┘  |
|                                 │ JNI                               |
|  ┌──────────────────────────────▼────────────────────────────────┐  |
|  │  plugins/engine-datafusion (Rust cdylib)                      │  |
|  │                                                               │  |
|  │  stats() → byte[] (protobuf-encoded)                          │  |
|  │    prost encodes all runtime + task monitor metrics           │  |
|  │                                                               │  |
|  └───────────────────────────────────────────────────────────────┘  |
+=====================================================================+
```

#### Pros

* **All 5.1 pros apply** — Single JNI call, protobuf, ServiceCache, classloader safe
* **Typed access for server consumers** — `NodeStats`, admission control, thread pool stats can read `DataFusionPluginStats` fields directly. No downcasting, no adapters.
* **Same pattern as OsStats/JvmStats** — Server holds concrete type, uses plain `Writeable`. Familiar to OpenSearch developers.
* **No NamedWriteableRegistry** — Eliminates the polymorphic serialization ceremony. Simpler wiring.
* **Cross-stat consumption** — Server consumers that need specific metric pools (e.g., Tokio worker counts for `thread_pool`) can access them directly from the typed stats object.

#### Cons

* **SPI grows with plugin schema** — `DataFusionPluginStats` and its `.proto` are now in the SPI module. Each new first-party plugin adds its concrete stats class to SPI. Acceptable for first-party engines tightly coupled to the server.
* **Proto dependency in SPI** — SPI's `build.gradle` needs protobuf codegen. SPI's Rust `Cargo.toml` already has prost.
* **Less decoupled** — Plugin schema is no longer fully owned by the plugin. But for first-party engines like DataFusion, full decoupling may not be necessary.


### 5.3 Approach Comparison

| # | Criteria | 5.1 Pull: Single stats(), PluginStats SPI | 5.2 Pull: Concrete Stats and Proto in SPI | Winner | Reason |
|---|---------|---------|---------|--------|--------|
| 1 | JNI direction | Java → Rust | Java → Rust | Tie | Both are pull-based, consumers control when metrics are collected |
| 2 | JNI calls per request | 1 per plugin | 1 per plugin | Tie | Single JNI call per plugin; byte[] with protobuf is compact and fast |
| 3 | Classloader safe | Yes | Yes | Tie | Both approaches handle classloader constraints correctly |
| 4 | Plugin isolation | Yes | Yes | Tie | Both approaches maintain proper plugin isolation |
| 5 | SPI surface area | Minimal (1 method + marker interface) | Larger (concrete stats class + proto in SPI) | 5.1 | Smallest contract between server and plugins |
| 6 | Typed access for server consumers | No (opaque PluginStats interface) | Yes (DataFusionPluginStats directly) | 5.2 | Server consumers get typed field access without downcasting |
| 7 | Read cost (consumer) | Nanoseconds (cached) | Nanoseconds (cached) | Tie | Both use ServiceCache with lazy refresh |
| 8 | NamedWriteableRegistry needed | Yes (polymorphic dispatch) | No (server knows concrete type) | 5.2 | Eliminates polymorphic serialization ceremony |
| 9 | Cross-stat consumption | Requires downcasting | Direct typed access | 5.2 | Server consumers (thread pool, admission control) can read specific metric pools directly |
| 10 | Schema ownership | Plugin owns its stats class entirely | SPI owns concrete stats class | 5.1 | Each plugin defines its own fields without SPI changes |
| 11 | Pattern familiarity | NamedWriteable (QueryBuilder, AggregationBuilder) | Plain Writeable (OsStats, JvmStats) | 5.2 | Same pattern as existing core stats classes |
| 12 | New plugin onboarding | Stats class in plugin + NamedWriteableRegistry entry | Stats class in SPI + proto in SPI | 5.1 | Less SPI changes needed per new plugin |
| 13 | Decoupling | Fully decoupled (plugin owns schema) | Less decoupled (schema in SPI) | 5.1 | Plugin schema is fully owned by the plugin |

_Note: Additional alternate approaches (MetricRegistry in Java, Periodic Push, MetricRegistry in Rust) are documented in the Appendix._

Recommendation: **5.2 Pull: Concrete Stats and Proto in SPI (Variant of 5.1)**. For first-party engines like DataFusion that are tightly coupled to the server, the benefits of typed access for server consumers, elimination of NamedWriteableRegistry ceremony, and alignment with the OsStats/JvmStats pattern outweigh the slightly larger SPI surface area. Full decoupling (5.1) remains a viable option for third-party plugins where the server should not know the concrete stats type. But this is currently out of scope.


## **_Please stop reading here._**

---

### Appendix

#### Adding a New Plugin (Checklist)

* Define a `.proto` schema for the plugin's metrics
* Add a single JNI function in Rust that returns protobuf-encoded `byte[]`
* Add a native method declaration in NativeBridge.java
* Create a PluginStats implementation (e.g. ParquetWriterPluginStats) — NamedWriteable + ToXContentFragment + decode(byte[])
* Create a MetricProvider implementation (e.g. ParquetWriterMetricProvider) — calls JNI, returns PluginStats
* Register NamedWriteableRegistry entry for the new stats class
* Add a @Nullable PluginStats field + ServiceCache\<PluginStats\> in NodeService

**No new Service or Probe classes.** Service+Probe boilerplate eliminated by ServiceCache.

### Alternate Approaches:

#### MetricRegistry in Java as Common Interface, MetricProvider in Rust (Not Recommended)

In this approach, the `MetricRegistry` lives in Java (SPI module) and each plugin registers a `MetricCollector` from its own classloader. The `MetricProvider` trait lives in Rust but each plugin implements it independently.

Filtering dimensions:

* **Stats filtering** — Which stats class to collect (e.g. DataFusion, ParquetWriter). Serialized to protobuf, passed across JNI.
* **Pool filtering** — Which metric pools within a stats class (e.g. io_runtime, cpu_runtime, query_execution). Serialized to protobuf, passed across JNI.
* **Metric filtering** — Which individual metrics within a pool (e.g. workers_count, queue_depth). Serialized to protobuf, passed across JNI.

Execution Flow:

```
Consumer builds MetricFilter (stats / pools / metrics)
    → MetricRegistry.collect(filter)                    [system classloader]
        → for each registered collector:
            collector.collectMetrics(filterBytes)        [plugin classloader]
                → JNI → Rust MetricProvider.collect(&filter)
                → protobuf bytes back to Java
                → ProtoDecoder.decode(bytes) → MetricStats
        → merge results from all plugins
    → MetricStats to consumer
```

Runtime Environment:

```
+=====================================================================+
|                               JVM PROCESS                           |
|                                                                     |
|   +-------------------------+                                       |
|   | System Classloader      |                                       |
|   |   MetricRegistry (Java) | <-- aggregation point                 |
|   |   MetricFilter          |                                       |
|   |   ProtoDecoder          |                                       |
|   |   VectorizedMetricStats |                                       |
|   +------------+------------+                                       |
|                |                                                    |
|                | holds collector references from each plugin        |
|                |                                                    |
|   +-------------------------+     +-------------------------+       |
|   | Plugin Classloader A    |     | Plugin Classloader B    |       |
|   |   DataFusionPlugin      |     |   ParquetWriterPlugin   |       |
|   |   NativeBridge ---------|--+  |   NativeBridge ---------|--+    |
|   +-------------------------+ |   +-------------------------+  |    |
|                               |                                |    |
+===============================|================================|====+
                                | JNI                            | JNI
                                v                                v
+-------------------------------+--+ +---------------------------+----+
| datafusion.dylib                 | | parquet.dylib                  |
| (Rust address space A)           | | (Rust address space B)         |
|                                  | |                                |
|  IO Runtime      CPU Runtime     | |  Writer Runtime                |
|  TaskMonitors                    | |  Flush Runtime                 |
|  DataFusionMetricProvider        | |  TaskMonitors                  |
|                                  | |  ParquetMetricProvider         |
+----------------------------------+ +--------------------------------+
```

Pros:

* **Classloader safe** — Each plugin's `NativeBridge` is loaded by its own plugin classloader
* **Plugin isolation** — Each plugin builds and loads its own .dylib/.so independently
* **Generic filtering** — Supports three dimensions: stats, pools, and metrics
* **Single aggregation point** — Java `MetricRegistry` merges results from all plugins
* **Protobuf as wire format** — Shared proto file between Rust (prost) and Java (protoc)

Cons:

* **Two-layer filtering** — Stats filtering in Java, pool/metric filtering in Rust
* **Proto dependency** — Both SPI jar and each plugin's Rust crate depend on the same .proto file
* **High JNI overhead** — High frequency of calls to MetricRegistry will require high number of JNI calls


#### Periodic Metric Push From Rust to JVM (Not Recommended)

In a push-based design, a Rust background task (Tokio spawned task) periodically snapshots metrics, encodes them as protobuf, and pushes the bytes into Java via reverse JNI. Java stores the latest snapshot in an `AtomicReference`. Consumers read from the cache without crossing JNI.

Execution Flow:

```
Rust Tokio task (every N ms)
    ↓
provider.collect(&MetricFilter::all()) → protobuf encode
    ↓
with_jni_env(|env| {
    env.call_static_method("MetricCache", "update", bytes)
})
    ↓ (inside Java)
ProtoDecoder.decode(bytes)
AtomicReference.set(stats)
    ↓
Consumers: MetricCache.get() → volatile read (~1ns)
```

Runtime Environment:

```
  +=====================================================================+
  |                               JVM PROCESS                           |
  |                                                                     |
  |   +-------------------------+                                       |
  |   | System Classloader      |                                       |
  |   |   MetricCache (Java)    | <-- AtomicReference<MetricStats>      |
  |   |   ProtoDecoder          |     volatile read by consumers        |
  |   |   MetricStats           |                                       |
  |   +------------+------------+                                       |
  |                ^                                                    |
  |                | MetricCache.update(bytes) via reverse JNI          |
  |                | (env.call_static_method from Rust thread)          |
  |                |                                                    |
  |   +-------------------------+     +-------------------------+       |
  |   | Plugin Classloader A    |     | Plugin Classloader B    |       |
  |   |   DataFusionPlugin      |     |   ParquetWriterPlugin   |       |
  |   |   NativeBridge          |     |   NativeBridge          |       |
  |   +-------------------------+     +-------------------------+       |
  |                                                                     |
  |   Consumers (read path — no JNI, no classloader involvement):       |
  |     NodeService.stats()          → MetricCache.get()  (~1ns)        |
  |     AdmissionControlService      → MetricCache.get()  (~1ns)        |
  |     SearchBackpressureService    → MetricCache.get()  (~1ns)        |
  |                                                                     |
  +=====================================================================+
        ^                                      ^
        | reverse JNI                          | reverse JNI
        | (with_jni_env / attach_as_daemon)    | (with_jni_env / attach_as_daemon)
        |                                      |
  +-----+----------------------------+  +------+---------------------------+
  | datafusion.dylib                 |  | parquet.dylib                    |
  | (Rust address space A)           |  | (Rust address space B)           |
  |                                  |  |                                  |
  |  JAVA_VM: OnceLock<JavaVM>       |  |  JAVA_VM: OnceLock<JavaVM>       |
  |  (shared with ActionListener     |  |  (shared with ActionListener     |
  |   callbacks)                     |  |   callbacks)                     |
  |                                  |  |                                  |
  |  ┌─────────────────────────┐     |  |  ┌─────────────────────────┐     |
  |  │ Tokio push task         │     |  |  │ Tokio push task         │     |
  |  │ (spawned on IO runtime) │     |  |  │ (spawned on IO runtime) │     |
  |  │                         │     |  |  │                         │     |
  |  │ loop {                  │     |  |  │ loop {                  │     |
  |  │   sleep(interval)       │     |  |  │   sleep(interval)       │     |
  |  │   provider.collect(all) │     |  |  │   provider.collect(all) │     |
  |  │   encode protobuf       │     |  |  │   encode protobuf       │     |
  |  │   with_jni_env(|env|    │     |  |  │   with_jni_env(|env|    │     |
  |  │     env.call_static(    │     |  |  │     env.call_static(    │     |
  |  │       "MetricCache",    │     |  |  │       "MetricCache",    │     |
  |  │       "update", bytes)  │     |  |  │       "update", bytes)  │     |
  |  │   )                     │     |  |  │   )                     │     |
  |  │ }                       │     |  |  │ }                       │     |
  |  └─────────────────────────┘     |  |  └─────────────────────────┘     |
  |                                  |  |                                  |
  |  IO Runtime      CPU Runtime     |  |  Writer Runtime                  |
  |  TaskMonitors                    |  |  Flush Runtime                   |
  |  DataFusionMetricProvider        |  |  TaskMonitors                    |
  |                                  |  |  ParquetMetricProvider           |
  |                                  |  |                                  |
  |  Shutdown ordering:              |  |  Shutdown ordering:              |
  |  1. cancel_token.cancel()        |  |  1. cancel_token.cancel()        |
  |  2. push_handle.await            |  |  2. push_handle.await            |
  |  3. cpu_executor.join_blocking() |  |  3. cpu_executor.join_blocking() |
  |  4. io_runtime drops (Arc)       |  |  4. io_runtime drops (Arc)       |
  +----------------------------------+  +----------------------------------+
  Thread attachment detail:
  - Push task calls attach_current_thread_as_daemon() (not permanently)
  - Daemon threads do NOT block DestroyJavaVM on JVM shutdown
  - If JVM tears down mid-push, the JNIEnv becomes invalid → task must
    check for JNI exceptions after every call and exit gracefully
  - Each plugin pushes independently; MetricCache.update() merges by
    provider name under a lock or ConcurrentHashMap.merge()
```

Pros:

* **Zero-cost reads** — `AtomicReference.get()` is a single volatile read (in nanoseconds, zero allocation)
* **Decouples collection from consumption** — For eg: Push once per second, consumes thousands per second.
* **Amortizes serialization** — One encode/decode per push interval
* **Reuses existing JavaVM infrastructure** — DataFusion plugin already has `with_jni_env`

Cons:

* **Reverse JNI lifecycle complexity** — Push task must be stopped before Tokio runtime shuts down
* **Wasted work when idle** — Push task runs even when no consumer reads metrics
* **No filter support on push path** — Push collects everything (`MetricFilter::all()`)
* **Multi-plugin merge complexity** — Each plugin pushes independently; Java must merge
* **Silent staleness on failure** — If Rust push task panics, cache goes stale silently


#### MetricRegistry in Rust (Not Recommended)

In this approach, both `MetricRegistry` and `MetricProvider` live entirely in Rust. The Java side would call a single JNI method that delegates to the Rust registry.

Execution Flow:

```
Consumer builds MetricFilter
    ↓
MetricsBridge.collectMetrics() [SPI jar, system classloader]
    ↓ static JNI method
Java_...MetricsBridge_collectMetrics [Rust, .dylib/.so]
    ↓
MetricRegistry (Rust)
    |--- provider A .collect()
    |--- provider B .collect()
    ↓
merged MetricCollection bytes
    ↓
ProtoDecoder.decode(bytes) [SPI jar]
    ↓
MetricStats
```

Runtime Environment:

```
  +=====================================================================+
  |                        JVM PROCESS                                  |
  |                                                                     |
  |   +-------------------------+                                       |
  |   | System Classloader      |                                       |
  |   |   MetricsBridge         |                                       |
  |   |   (static JNI method)   |                                       |
  |   +------------+------------+                                       |
  |                |                                                    |
  |                | calls JNI                                          |
  |                v                                                    |
  |          X  UnsatisfiedLinkError                                    |
  |          (no .dylib bound to system classloader)                    |
  |                                                                     |
  |   +-------------------------+     +-------------------------+       |
  |   | Plugin Classloader A    |     | Plugin Classloader B    |       |
  |   |   DataFusionPlugin      |     |   ParquetWriterPlugin   |       |
  |   |   NativeBridge          |     |   NativeBridge          |       |
  |   +------------+------------+     +------------+------------+       |
  |                |                               |                    |
  +================|===============================|====================+
                   | JNI                           | JNI
                   v                               v
  +----------------+---------------+ +-------------+----------------+
  | datafusion.dylib               | | parquet.dylib                |
  | (Rust address space A)         | | (Rust address space B)       |
  |                                | |                              |
  |  IO Runtime    CPU Runtime     | |  Writer Runtime              |
  |  TaskMonitors                  | |  Flush Runtime               |
  |  MetricProvider A              | |  TaskMonitors                |
  |                                | |  MetricProvider B            |
  +--------------------------------+ +------------------------------+
```

Pros:

* **Single JNI call** — One JNI call regardless of plugin count
* **Merging happens in Rust** — Faster, avoids Java heap allocations
* **Simple Java side** — Minimal logic: one call, one decode, one result
* **Reduced JNI overhead** — One boundary crossing per stats request

Cons:

* **Classloader mismatch** — `MetricsBridge` is in the SPI jar (system classloader), but the shared library is loaded by the plugin classloader. This produces `UnsatisfiedLinkError` at runtime.
* **Single .dylib assumption** — Assumes all providers are linked into the same shared library, but each plugin loads its own .dylib/.so
* **Compilation coupling** — The SPI Rust crate would need to link against every plugin's Rust code at build time

#### Design Review Comments (from Quip)

**Thread 1** — on JNI data transfer volume
> **Himshikha Gupta:** The amount of data being transferred remains the same right?
> **Ajay Raj Nelapudi:** Yes, total data remains the same. No of JNI crossings will be more though.

**Thread 2** — on filtering need
> **Himshikha Gupta:** Where will this be needed?
> **Ajay Raj Nelapudi:** Since we're providing a single interface across all metrics, it might not be necessary to fetch all metrics (across all plugins) when only a few metrics are required.
> **Arpit Bandejiya:** this can be done by rest action right? How the node stats can give different response depending on the path

**Thread 3** — on Rust registry viability with plugin isolation
> **Himshikha Gupta:** Then this doesnt work?
> **Ajay Raj Nelapudi:** Yes, this will not work with plugin isolation.

**Thread 4** — on BeagleStone collector intervals
> **Himshikha Gupta:** BS collector will always read in fixed intervals?
> **Ajay Raj Nelapudi:** Yes, most BS collectors runs per minute, with carbon specifc collectors running at once per 15s interval.

**Thread 5** — on failure handling in push approach
> **Himshikha Gupta:** We can add handling for this right? Even pull mechanisms can fail and we would need to handle those failures gracefully.

**Thread 6** — on cache refresh requiring JNI
> **Himshikha Gupta:** cache population will also need a JNI call?
> **Ajay Raj Nelapudi:** Yes, refreshes will require JNI call.

**Thread 7** — on scheduled task frequency and native engines
> **Himshikha Gupta:** scheduled what?
> **Himshikha Gupta:** whats the frequency of this?
> **Ajay Raj Nelapudi:** Scheduled task of 1s.
> **Bharathwaj G:** How does this work with native engines?
> **Ajay Raj Nelapudi:** Goal is to inject a MetricProvider into specific collectors like AC/BP and have them fetch the metrics themselves. We will not try to maintain a cache or layer.

**Thread 8** — on current metric collection
> **Arpit Bandejiya:** How are they being collected right now? And how does it integrate with these consumers

**Thread 9** — on rayon thread metrics priority
> **Arpit Bandejiya:** Maybe a P1, we can account for merge rayon threads as well?
> **Ajay Raj Nelapudi:** Tokio runtime and task metrics should be P0 for fetching threadpool related metrics, isn't it? Rayon threads can be P1.
> **Bharathwaj G:** why are we considering rayon as p1? if rayon is implemented, metrics should be p0 as well na
> **Ajay Raj Nelapudi:** We can explore P0 and P1 as a part of stats API design review that I'll setup shortly.

**Thread 10** — on plugins registering REST actions
> **Arpit Bandejiya:** Plugins can register their own rest actions for this?
> **Ajay Raj Nelapudi:** Yes but this requires making a separate REST call to the plugin. This will be added latency for _node/stats API and existing consumers like AC, BP, etc.

**Thread 11** — on Rust-side state
> **Arpit Bandejiya:** Will we be maintaining a state in the Rust side as well?

**Thread 12** — on alternate approach using existing OS/JVM services
> **Himshikha Gupta:** An alternate to having a single metric collector and then filtering, could also be plugging the existing classes like OSService to collect metrics from Rust, and have new NativeMemoryService for tracking memory. The flip side would be each service needs to keep track of where all to collect from..

**Thread 13** — on removing non-viable approach
> **Himshikha Gupta:** Lets remove this if we already know this is not viable?
> **Ajay Raj Nelapudi:** Moved it to alternate approaches section

**Thread 14** — on Arrow as intermediate format
> **Arpit Bandejiya:** Will Arrow work as an intermediate format for transferring info?
> **Arpit Bandejiya:** Will this impact latency?
> **Ajay Raj Nelapudi:** As metric size grows (eg: per task metrics) arrow makes sense. I updated Approach 5.3 and made it recommended instead of this approach.
> **Ajay Raj Nelapudi:** And also yes to proto increasing latency due to ser/de costs for large set of metrics.

**Thread 15** — on metric scope (CPU/Memory)
> **Himshikha Gupta:** Is the design scope limited to these metrics? Can we atleast include CPU and Memory which we already know are going to be key metrics.
> **Ajay Raj Nelapudi:** Added CPU and Memory to the list. We'll be emitting them once PoC is finalized.

**Thread 16** — on "visibility"
> **Himshikha Gupta:** What kind of visibility?
> **Ajay Raj Nelapudi:** metric visibility

**Thread 17** — on metric categories
> **Himshikha Gupta:** What do we mean by metric categories?

**Thread 18** — on filtering definition and use case
> **Himshikha Gupta:** Would be good to define what we mean by filtering, and what is the usecase to support this
> **Ajay Raj Nelapudi:** Changed the wording for better understanding.

**Thread 19** — on CachedService replacing existing classes
> **Himshikha Gupta:** Didnt get this. Are we changing existing classes?
> **Ajay Raj Nelapudi:** Currently, for each metric we expose we write a Service (top layer with cache), Probe (actual fetcher), Stats (formatter) - most of it being boilderplate code. We'll instead have a CachedService class that implements all these so we can reduce boilerplate code as we add more metrics.
> **Arpit Bandejiya:** Didn't get why this change is needed. Can we talk about how this requirement is coming in?

**Thread 20** — on serialization location
> **Himshikha Gupta:** Does this mean we have to go to Rust to serialize and deserialize?
> **Ajay Raj Nelapudi:** This happens on Java, particularly for stats API. It's an existing pattern.

**Thread 21** — on JNI return type
> **Himshikha Gupta:** What are we planning to return? Array of metrics or pointer to the array?
> **Ajay Raj Nelapudi:** It'll be a double array / double[].

**Thread 22** — on stats API knowing about IO/CPU runtimes
> **Himshikha Gupta:** Why does stats API need to understand IORuntime and CPURuntime? Should it just ask for threadpool metrics?
> **Ajay Raj Nelapudi:** Stats API will simply call the service which in turn contain providers for specific metrics.
> **Ajay Raj Nelapudi:** Slightly above in the diagram, we see stats API just calls runtimeCacheService.stats() and everything else is handled by metricprovider and plugin.

**Thread 23** — on comparison table winners
> **Himshikha Gupta:** Can we add what we see as a winner in this comparison for each category?
> **Ajay Raj Nelapudi:** Added winner and reason to each criteria.

**Thread 24** — on approach ordering
> **Himshikha Gupta:** nit: Would be good to keep the preffered approach first, so we can focus major discussion on that. Other two can be alternate approaches.
> **Ajay Raj Nelapudi:** Makes sense, moved the recommend approach to the top

**Thread 25** — on scoping metrics
> **Arpit Bandejiya:** Have we scoped down what all metrics we will end up changing?
> **Ajay Raj Nelapudi:** No, that discussion is separate. Will be part of stats API, still in draft state - https://quip-amazon.com/lFJqA4fRQR38. I will setup a separate call.

**Thread 26** — on category definition
> **Himshikha Gupta:** how do we define category?
> **Ajay Raj Nelapudi:** Category might not be the right word here but the idea of this approach is to expose JNI functions (and providers) based on the callers. 1. Entire stats can be one JNI function 2. But specific JNI calls can be offered for per-task queries.

**Thread 27** — on classloader section relevance
> **Bukhtawar Khan:** Sorry how is this relevant, thats how plugins are loaded by the plugin class loader
> **Ajay Raj Nelapudi:** I think the changes came because of a PoC I did assuming all plugins will share an address space and tried to have a common metric pool. Please ignore for now.

**Thread 28** — on node-level vs per-index/shard stats
> **Arpit Bandejiya:** So this will be at node level? How will that work when we want to store stats per index per shard?

**Thread 29** — on double[] JNI benchmarks
> **Bharathwaj G:** any benchmarks / references to back this? generally haven't seen double as the main object for transfer in JNI

**Thread 30** — on stats volume
> **Bharathwaj G:** do we need this many stats back to node stats? maybe we should have small, important set and then expand
> **Ajay Raj Nelapudi:** We can trim this down. I will get back with required metrics.

**Thread 31** — on generic MetricProvider interface
> **Bharathwaj G:** metrics provider will need generic interface right? get runtime metrics sound specific
> **Ajay Raj Nelapudi:** Right. We can do something like MetricProvider.getMetrics(...filter) - interface. All metric providers can implement this and route to specific calls internally.

**Thread 32** — on SPI design
> **Arpit Bandejiya:** Can we talk about how this SPI will look like?
> **Ajay Raj Nelapudi:** Making a few updates here.

**Thread 33** — on doc/reader/searcher metrics
> **Bharathwaj G:** how will you get these metrics? also doc metrics etc which require actual reader / searcher?
> **Ajay Raj Nelapudi:** Will need searcher and reader

**Thread 34** — on unclear statement
> **Arpit Bandejiya:** What does this mean?

**Thread 35** — on stats holder interface (fs probe pattern)
> **Bharathwaj G:** I think we should use something similar to fs probe, where each service holds the stats which gets updated periodically and when node stats is called, it gets returned back. I'm not seeing the stats holder interface anywhere.

**Thread 36** — on capturing decisions
> **Bharathwaj G:** Is this captured somewhere?

**Thread 37** — on cachedService.stats() granularity
> **Suresh N S:** In the new approach, when we call cachedService.stats(), will it be possible to retrieve the stats for a particular category alone? (Categories like JVM, OS, etc.) Or will it be always all stats together?
> **Ajay Raj Nelapudi:** .stats() is typically used by _stats API to fetch all metrics but stats() is a lyer over MetricProvider which can expose functions for granular metrics.
> **Arpit Bandejiya:** The Services are capturing for the node level stats. This can be done for native memory across the component. What about the Index specific metrics? Are you saying the same service will serve it?

**Thread 38** — on TokioWrapper
> **Ajay Raj Nelapudi:** Explore TokioWrapper with central instrumentation
