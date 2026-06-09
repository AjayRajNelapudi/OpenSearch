/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action that fans out task dump requests to target nodes in the cluster.
 *
 * <p>Extends {@link TransportNodesAction} to leverage the standard node fan-out, failure handling,
 * and serialization infrastructure. On each target node, calls
 * {@link DataFusionService#getTaskDump(boolean, long)} to obtain the tokio task dump JSON.
 *
 * @opensearch.internal
 */
public class TransportTaskDumpAction extends TransportNodesAction<
    TaskDumpNodesRequest,
    TaskDumpNodesResponse,
    TaskDumpNodeRequest,
    TaskDumpNodeResponse> {

    private final DataFusionService dataFusionService;

    @Inject
    public TransportTaskDumpAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DataFusionService dataFusionService
    ) {
        super(
            TaskDumpActionType.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            TaskDumpNodesRequest::new,
            TaskDumpNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            TaskDumpNodeResponse.class
        );
        this.dataFusionService = dataFusionService;
    }

    @Override
    protected TaskDumpNodesResponse newResponse(
        TaskDumpNodesRequest request,
        List<TaskDumpNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new TaskDumpNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected TaskDumpNodeRequest newNodeRequest(TaskDumpNodesRequest request) {
        return new TaskDumpNodeRequest(request);
    }

    @Override
    protected TaskDumpNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new TaskDumpNodeResponse(in);
    }

    @Override
    protected TaskDumpNodeResponse nodeOperation(TaskDumpNodeRequest request) {
        String taskDumpJson;
        try {
            taskDumpJson = dataFusionService.getTaskDump(request.isSummaryOnly(), request.getLimit());
        } catch (IllegalStateException e) {
            // DataFusionService not started on this node — return null JSON
            taskDumpJson = null;
        }
        return new TaskDumpNodeResponse(clusterService.localNode(), taskDumpJson);
    }
}
