/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Metrics collection for tokio runtimes.

pub mod collector;

pub use collector::{MetricsCollector, MetricsSnapshot};

