/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for DataFusion task dump that targets one or more nodes.
 *
 * <p>Extends {@link BaseNodesRequest} to carry the {@code summaryOnly} and {@code limit}
 * parameters through the transport layer to each target node.
 *
 * @opensearch.internal
 */
public class TaskDumpNodesRequest extends BaseNodesRequest<TaskDumpNodesRequest> {

    private final boolean summaryOnly;
    private final long limit;

    /**
     * Creates a new request targeting the specified nodes with the given parameters.
     *
     * @param nodesIds    the node IDs to target (empty array means all nodes)
     * @param summaryOnly when true, omit per-task traces from the response
     * @param limit       max tasks in the traces array; -1 means no limit
     */
    public TaskDumpNodesRequest(String[] nodesIds, boolean summaryOnly, long limit) {
        super(nodesIds);
        this.summaryOnly = summaryOnly;
        this.limit = limit;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public TaskDumpNodesRequest(StreamInput in) throws IOException {
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
