/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.taskdump;

import org.opensearch.action.ActionType;

/**
 * Action type for the DataFusion task dump transport action.
 *
 * <p>Singleton constant identifying the transport action that collects
 * tokio task dump diagnostics from all (or specific) nodes in the cluster.
 *
 * @opensearch.internal
 */
public class TaskDumpActionType extends ActionType<TaskDumpNodesResponse> {

    public static final String NAME = "cluster:monitor/_analytics_backend_datafusion/task_dump";
    public static final TaskDumpActionType INSTANCE = new TaskDumpActionType();

    private TaskDumpActionType() {
        super(NAME, TaskDumpNodesResponse::new);
    }
}
