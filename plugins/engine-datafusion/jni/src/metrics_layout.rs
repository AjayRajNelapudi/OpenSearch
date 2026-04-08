/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Flat `long[]` layout constants shared between Rust JNI writer and Java decoder.
//!
//! These offsets MUST stay in sync with the Java `RuntimeValues` and
//! `TaskMonitorValues` offset constants.

// ── RuntimeValues offsets (per-runtime block) ──

pub const RUNTIME_WORKERS_COUNT: usize = 0;
pub const RUNTIME_TOTAL_POLLS_COUNT: usize = 1;
pub const RUNTIME_TOTAL_BUSY_DURATION_MS: usize = 2;
pub const RUNTIME_TOTAL_OVERFLOW_COUNT: usize = 3;
pub const RUNTIME_GLOBAL_QUEUE_DEPTH: usize = 4;
pub const RUNTIME_BLOCKING_QUEUE_DEPTH: usize = 5;
pub const RUNTIME_MAX_GLOBAL_QUEUE_DEPTH: usize = 6;
pub const RUNTIME_P50_GLOBAL_QUEUE_DEPTH: usize = 7;
pub const RUNTIME_P90_GLOBAL_QUEUE_DEPTH: usize = 8;
pub const RUNTIME_P99_GLOBAL_QUEUE_DEPTH: usize = 9;
pub const RUNTIME_NUM_ALIVE_TASKS: usize = 10;
pub const RUNTIME_SPAWNED_TASKS_COUNT: usize = 11;
pub const RUNTIME_REMOTE_SCHEDULE_COUNT: usize = 12;
pub const RUNTIME_BUDGET_FORCED_YIELD_COUNT: usize = 13;
pub const RUNTIME_NUM_BLOCKING_THREADS: usize = 14;
pub const RUNTIME_NUM_IDLE_BLOCKING_THREADS: usize = 15;
pub const RUNTIME_TOTAL_PARK_COUNT: usize = 16;
pub const RUNTIME_TOTAL_STEAL_COUNT: usize = 17;
pub const RUNTIME_TOTAL_NOOP_COUNT: usize = 18;
pub const RUNTIME_TOTAL_STEAL_OPERATIONS: usize = 19;
pub const RUNTIME_TOTAL_LOCAL_SCHEDULE_COUNT: usize = 20;
pub const RUNTIME_TOTAL_LOCAL_QUEUE_DEPTH: usize = 21;
pub const RUNTIME_SIZE: usize = 22;

// ── TaskMonitorValues offsets (per-monitor block) ──

pub const TASK_MONITOR_TOTAL_POLL_DURATION_MS: usize = 0;
pub const TASK_MONITOR_TOTAL_SCHEDULED_DURATION_MS: usize = 1;
pub const TASK_MONITOR_TOTAL_IDLE_DURATION_MS: usize = 2;
pub const TASK_MONITOR_REJECTED: usize = 3;
pub const TASK_MONITOR_INSTRUMENTED_COUNT: usize = 4;
pub const TASK_MONITOR_DROPPED_COUNT: usize = 5;
pub const TASK_MONITOR_FIRST_POLL_COUNT: usize = 6;
pub const TASK_MONITOR_TOTAL_FIRST_POLL_DELAY_MS: usize = 7;
pub const TASK_MONITOR_TOTAL_IDLED_COUNT: usize = 8;
pub const TASK_MONITOR_TOTAL_SCHEDULED_COUNT: usize = 9;
pub const TASK_MONITOR_TOTAL_POLL_COUNT: usize = 10;
pub const TASK_MONITOR_TOTAL_FAST_POLL_COUNT: usize = 11;
pub const TASK_MONITOR_TOTAL_FAST_POLL_DURATION_MS: usize = 12;
pub const TASK_MONITOR_TOTAL_SLOW_POLL_COUNT: usize = 13;
pub const TASK_MONITOR_TOTAL_SLOW_POLL_DURATION_MS: usize = 14;
pub const TASK_MONITOR_TOTAL_SHORT_DELAY_COUNT: usize = 15;
pub const TASK_MONITOR_TOTAL_LONG_DELAY_COUNT: usize = 16;
pub const TASK_MONITOR_TOTAL_SHORT_DELAY_DURATION_MS: usize = 17;
pub const TASK_MONITOR_TOTAL_LONG_DELAY_DURATION_MS: usize = 18;
pub const TASK_MONITOR_SIZE: usize = 19;

// ── Total flat array size: 2 runtimes × 22 + 5 task monitors × 19 = 139 ──

pub const TOTAL_SIZE: usize = RUNTIME_SIZE * 2 + TASK_MONITOR_SIZE * 5;

// Compile-time assertion
const _: () = assert!(TOTAL_SIZE == 139, "TOTAL_SIZE must be 139");
