/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Per-node request sent to each target node during the task dump fan-out.
 *
 * <p>Carries the {@code summaryOnly} and {@code limit} parameters from the parent
 * {@link TaskDumpNodesRequest} so each node knows how to produce its dump.
 *
 * @opensearch.internal
 */
public class TaskDumpNodeRequest extends TransportRequest {

    private final boolean summaryOnly;
    private final long limit;

    /**
     * Constructs a node request from the parent nodes request, extracting both fields.
     *
     * @param nodesRequest the parent cluster-level request
     */
    public TaskDumpNodeRequest(TaskDumpNodesRequest nodesRequest) {
        this.summaryOnly = nodesRequest.isSummaryOnly();
        this.limit = nodesRequest.getLimit();
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public TaskDumpNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.summaryOnly = in.readBoolean();
        this.limit = in.readLong();
    }

    /**
     * Returns whether only the summary should be included (omitting per-task traces).
     */
    public boolean isSummaryOnly() {
        return summaryOnly;
    }

    /**
     * Returns the maximum number of task traces to include. -1 means no limit.
     */
    public long getLimit() {
        return limit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(summaryOnly);
        out.writeLong(limit);
    }
}
