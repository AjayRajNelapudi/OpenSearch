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
pub const RUNTIME_SIZE: usize = 10;

// ── TaskMonitorValues offsets (per-monitor block) ──

pub const TASK_MONITOR_TOTAL_POLL_DURATION_MS: usize = 0;
pub const TASK_MONITOR_TOTAL_SCHEDULED_DURATION_MS: usize = 1;
pub const TASK_MONITOR_TOTAL_IDLE_DURATION_MS: usize = 2;
pub const TASK_MONITOR_REJECTED: usize = 3;
pub const TASK_MONITOR_SIZE: usize = 4;

// ── Total flat array size: 2 runtimes × 10 + 5 task monitors × 4 = 40 ──

pub const TOTAL_SIZE: usize = RUNTIME_SIZE * 2 + TASK_MONITOR_SIZE * 5;

// Compile-time assertion that TOTAL_SIZE == 40
const _: () = assert!(TOTAL_SIZE == 40, "TOTAL_SIZE must be 40");
