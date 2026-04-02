/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Extends {@link ResizableBlockingQueue} with a pluggable chain of
 * {@link QueueRejectionCheck} pre-checks that run inside {@link #offer(Object)}
 * before delegating to {@code super.offer()}. If any check rejects, {@code offer()}
 * returns false immediately, triggering the existing {@code OpenSearchAbortPolicy}.
 * <p>
 * The queue itself is a thin orchestrator — all rejection logic lives in
 * {@link QueueRejectionCheck} implementations. Checks are iterated in order
 * (cheapest first) and short-circuit on the first rejection.
 * <p>
 * {@link #forcePut(Object)} is inherited from {@link SizeBlockingQueue} and bypasses
 * the rejection checks, so force-execution tasks are never rejected by this queue.
 * {@link #adjustCapacity(int, int, int, int)} is inherited from {@link ResizableBlockingQueue}
 * and continues to work unchanged.
 *
 * @opensearch.internal
 */
final class CompositeResizableBlockingQueue<E> extends ResizableBlockingQueue<E> {

    private final List<QueueRejectionCheck> rejectionChecks;

    CompositeResizableBlockingQueue(BlockingQueue<E> queue, int initialCapacity, List<QueueRejectionCheck> rejectionChecks) {
        super(queue, initialCapacity);
        this.rejectionChecks = List.copyOf(rejectionChecks);
    }

    @Override
    public boolean offer(E e) {
        // Iterate all rejection checks — cheapest first
        for (QueueRejectionCheck check : rejectionChecks) {
            if (check.shouldReject()) {
                return false;  // triggers OpenSearchAbortPolicy
            }
        }
        // All checks passed — delegate to standard ResizableBlockingQueue offer (capacity + CAS)
        return super.offer(e);
    }
}
