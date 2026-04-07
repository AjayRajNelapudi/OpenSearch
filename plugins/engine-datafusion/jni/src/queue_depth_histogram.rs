/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Lock-free ring buffer for tracking global_queue_depth percentiles.
//!
//! Samples are recorded at each `is_saturated()` call (JNI entry points).
//! Percentiles are computed at snapshot time by copying + sorting the buffer.
//! The buffer uses a wrapping atomic index for writes; readers accept that
//! a few entries may be mid-write (stale values from previous cycle), which
//! is acceptable for percentile approximation.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Number of samples retained in the ring buffer.
const CAPACITY: usize = 10_240;

/// A fixed-size circular buffer of u64 samples with lock-free writes
/// and snapshot-based percentile computation.
pub struct QueueDepthHistogram {
    /// Ring buffer storage. Each slot is an AtomicU64 to allow concurrent
    /// reads during snapshot without data races (though values may be stale).
    samples: Box<[AtomicU64; CAPACITY]>,
    /// Monotonically increasing write index (wraps via modulo).
    write_idx: AtomicUsize,
}

impl QueueDepthHistogram {
    /// Creates a new histogram with all samples initialized to 0.
    pub fn new() -> Self {
        // Initialize array of AtomicU64 — can't use array init syntax for large arrays
        let samples: Vec<AtomicU64> = (0..CAPACITY).map(|_| AtomicU64::new(0)).collect();
        let boxed: Box<[AtomicU64; CAPACITY]> = samples.into_boxed_slice().try_into().unwrap();
        Self {
            samples: boxed,
            write_idx: AtomicUsize::new(0),
        }
    }

    /// Records a single queue depth observation. Called on the hot path
    /// (JNI entry points), so this must be fast: one atomic increment +
    /// one atomic store.
    pub fn record(&self, depth: u64) {
        let idx = self.write_idx.fetch_add(1, Ordering::Relaxed) % CAPACITY;
        self.samples[idx].store(depth, Ordering::Relaxed);
    }

    /// Computes percentiles from the current buffer contents.
    /// Returns (p50, p90, p99) as u64 values.
    ///
    /// This copies the entire buffer (~80KB), sorts it, and picks
    /// percentile indices. Takes ~100-200μs for 10K elements.
    /// Called once per stats API request (every ~1s), not on the hot path.
    pub fn percentiles(&self) -> (u64, u64, u64) {
        let count = self.write_idx.load(Ordering::Relaxed).min(CAPACITY);
        if count == 0 {
            return (0, 0, 0);
        }

        // Copy samples into a sortable vec
        let mut buf: Vec<u64> = Vec::with_capacity(count);
        for i in 0..count {
            buf.push(self.samples[i].load(Ordering::Relaxed));
        }
        buf.sort_unstable();

        let p50 = buf[percentile_index(count, 50)];
        let p90 = buf[percentile_index(count, 90)];
        let p99 = buf[percentile_index(count, 99)];
        (p50, p90, p99)
    }
}

/// Returns the array index for the given percentile (0-99).
fn percentile_index(count: usize, pct: usize) -> usize {
    ((count * pct) / 100).min(count - 1)
}
