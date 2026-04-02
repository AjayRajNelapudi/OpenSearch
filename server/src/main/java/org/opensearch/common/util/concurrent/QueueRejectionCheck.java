/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

/**
 * A stateless pre-check that can reject a task before it enters the queue.
 * Implementations should be cheap (no locks, no JNI, no blocking).
 * Used by the composite resizable blocking queue to iterate a chain of rejection
 * checks before delegating to {@code super.offer()}.
 *
 * @opensearch.internal
 */
@FunctionalInterface
interface QueueRejectionCheck {
    /** Returns {@code true} if the task should be rejected. */
    boolean shouldReject();
}
