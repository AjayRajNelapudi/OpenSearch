/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Per-node response carrying that node's task dump as an opaque JSON string.
 *
 * <p>Extends {@link BaseNodeResponse} to participate in the {@code TransportNodesAction} framework.
 * The JSON payload is nullable — a null value indicates the service was unavailable on this node.
 *
 * <p>Wire format:
 * <pre>
 * [BaseNodeResponse fields: DiscoveryNode]
 * [Optional String: taskDumpJson via writeOptionalString/readOptionalString]
 * </pre>
 *
 * @opensearch.internal
 */
public class TaskDumpNodeResponse extends BaseNodeResponse {

    private final String taskDumpJson;

    /**
     * Construct a node response with the given node and task dump JSON.
     *
     * @param node         the discovery node this response is from
     * @param taskDumpJson the task dump JSON for this node (may be null if unavailable)
     */
    public TaskDumpNodeResponse(DiscoveryNode node, String taskDumpJson) {
        super(node);
        this.taskDumpJson = taskDumpJson;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public TaskDumpNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.taskDumpJson = in.readOptionalString();
    }

    /**
     * Returns the task dump JSON for this node, or {@code null} if unavailable.
     */
    public String getTaskDumpJson() {
        return taskDumpJson;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(taskDumpJson);
    }
}
