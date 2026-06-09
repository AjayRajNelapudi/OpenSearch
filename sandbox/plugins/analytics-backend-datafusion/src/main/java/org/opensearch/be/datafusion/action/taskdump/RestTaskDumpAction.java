/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for the DataFusion task dump diagnostic endpoint.
 *
 * <p>Exposes the tokio CPU runtime task dump with async stack traces.
 * Supports {@code ?summary_only=true} to omit per-task traces and
 * {@code ?limit=N} to cap the number of task traces returned.
 *
 * <p>Route:
 * <ul>
 *   <li>{@code GET /_plugins/_analytics_backend_datafusion/task_dump}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class RestTaskDumpAction extends BaseRestHandler {

    private static final String CANONICAL_PREFIX = "/_plugins/_analytics_backend_datafusion";

    @Override
    public String getName() {
        return "datafusion_task_dump_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, CANONICAL_PREFIX + "/task_dump"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean summaryOnly = request.paramAsBoolean("summary_only", false);

        long limit = -1;
        String limitParam = request.param("limit");
        if (limitParam != null) {
            try {
                limit = Long.parseLong(limitParam);
            } catch (NumberFormatException e) {
                return channel -> {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("error", "Invalid 'limit' parameter: must be a non-negative integer. Got: " + limitParam);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
                };
            }
            if (limit < 0) {
                long finalLimit = limit;
                return channel -> {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("error", "Invalid 'limit' parameter: must be non-negative. Got: " + finalLimit);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
                };
            }
        }

        TaskDumpNodesRequest nodesRequest = new TaskDumpNodesRequest(new String[0], summaryOnly, limit);

        return channel -> client.execute(
            TaskDumpActionType.INSTANCE,
            nodesRequest,
            new RestBuilderListener<TaskDumpNodesResponse>(channel) {
                @Override
                public org.opensearch.rest.RestResponse buildResponse(TaskDumpNodesResponse response, XContentBuilder builder)
                    throws Exception {
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            }
        );
    }
}
