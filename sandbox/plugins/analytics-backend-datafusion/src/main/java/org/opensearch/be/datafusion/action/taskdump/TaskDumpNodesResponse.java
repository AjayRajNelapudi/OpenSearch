/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregated cluster-wide response for DataFusion task dump.
 *
 * <p>Extends {@link BaseNodesResponse} and implements {@link ToXContentFragment}
 * to render the full JSON structure including {@code _nodes}, {@code cluster_name},
 * {@code cluster_total_tasks}, and per-node task dump data keyed by node ID.
 *
 * <p>JSON structure:
 * <pre>
 * {
 *   "_nodes": { "total": N, "successful": S, "failed": F, "failures": [...] },
 *   "cluster_name": "...",
 *   "cluster_total_tasks": 8942,
 *   "nodes": {
 *     "node-id-1": {
 *       "num_tasks": 2800,
 *       "summary": { "by_location": { ... } },
 *       "tasks": [ ... ]
 *     }
 *   }
 * }
 * </pre>
 *
 * @opensearch.internal
 */
public class TaskDumpNodesResponse extends BaseNodesResponse<TaskDumpNodeResponse> implements ToXContentFragment {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Construct a nodes response with the given cluster name, successful node responses, and failures.
     *
     * @param clusterName the cluster name
     * @param nodes       the list of successful per-node responses
     * @param failures    the list of failed node exceptions
     */
    public TaskDumpNodesResponse(ClusterName clusterName, List<TaskDumpNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public TaskDumpNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<TaskDumpNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(TaskDumpNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<TaskDumpNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        RestActions.buildNodesHeader(builder, params, this);
        builder.field("cluster_name", getClusterName().value());

        // Sum cluster_total_tasks from each node's JSON
        long clusterTotalTasks = 0;
        for (TaskDumpNodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getTaskDumpJson() != null) {
                Map<String, Object> nodeMap = MAPPER.readValue(nodeResponse.getTaskDumpJson(), Map.class);
                Number numTasks = (Number) nodeMap.get("num_tasks");
                if (numTasks != null) {
                    clusterTotalTasks += numTasks.longValue();
                }
            }
        }
        builder.field("cluster_total_tasks", clusterTotalTasks);

        builder.startObject("nodes");
        for (TaskDumpNodeResponse nodeResponse : getNodes()) {
            builder.startObject(nodeResponse.getNode().getId());
            if (nodeResponse.getTaskDumpJson() != null) {
                Map<String, Object> nodeMap = MAPPER.readValue(nodeResponse.getTaskDumpJson(), Map.class);
                for (Map.Entry<String, Object> entry : nodeMap.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
