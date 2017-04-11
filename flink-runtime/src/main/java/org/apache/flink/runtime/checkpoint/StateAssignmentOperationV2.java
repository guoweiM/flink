/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.migration.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
public class StateAssignmentOperationV2 {

	private final Logger logger;
	private final Map<JobVertexID, ExecutionJobVertex> tasks;
	private final Map<JobVertexID, TaskState> taskStates;
	private final boolean allowNonRestoredState;

	public StateAssignmentOperationV2(
			Logger logger,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			Map<JobVertexID, TaskState> taskStates,
			boolean allowNonRestoredState) {

		this.logger = Preconditions.checkNotNull(logger);
		this.tasks = Preconditions.checkNotNull(tasks);
		this.taskStates = Preconditions.checkNotNull(taskStates);
		this.allowNonRestoredState = allowNonRestoredState;
	}

	public boolean assignStates() throws Exception {
		Map<JobVertexID, ExecutionJobVertex> localTasks = this.tasks;

		/** the previous version of this class can be found at {@link StateAssignmentOperation} */
		for (Map.Entry<JobVertexID, ExecutionJobVertex> task : localTasks.entrySet()) {
			final ExecutionJobVertex executionJobVertex = task.getValue();

			// find the states of all operators belonging to this task
			JobVertexID[] operatorIDs = executionJobVertex.getOperatorIDs();
			List<TaskState> operatorStates = new ArrayList<>();
			for (JobVertexID operatorID : operatorIDs) {
				TaskState operatorState = taskStates.get(operatorID);
				if (operatorState == null) {
					operatorState = new TaskState(
						operatorID,
						executionJobVertex.getParallelism(),
						executionJobVertex.getMaxParallelism(),
						1);
				}
				operatorStates.add(operatorState);
			}

			//TODO: add new restore logic

			// this is a simplified version to verify that the changes so far are correct
			// this will only work for non-keyed state without any parallelism change
			for (int subTaskIdx = 0; subTaskIdx < task.getValue().getParallelism(); subTaskIdx++) {
				Execution currentExecutionAttempt = executionJobVertex
					.getTaskVertices()[subTaskIdx]
					.getCurrentExecutionAttempt();

				List<StreamStateHandle> nonPartitionableState = new ArrayList<>();
				List<Collection<OperatorStateHandle>> operatorStateFromBackend = new ArrayList<>();
				List<Collection<OperatorStateHandle>> operatorStateFromStream = new ArrayList<>();
				KeyedStateHandle newKeyedStatesBackend = null;
				KeyedStateHandle newKeyedStateStream = null;

				for (int x = 0; x < operatorStates.size(); x++) {
					TaskState operatorState = operatorStates.get(x);
					SubtaskState subtaskOperatorState = operatorState.getState(subTaskIdx);
					if (subtaskOperatorState == null) {
						subtaskOperatorState = new SubtaskState(
							new ChainedStateHandle<>(
								Collections.<StreamStateHandle>emptyList()),
								null,
								null,
								null,
								null);
					}

					if (x == 0) {
						newKeyedStatesBackend = subtaskOperatorState.getManagedKeyedState();
						newKeyedStateStream = subtaskOperatorState.getRawKeyedState();
					}
					ChainedStateHandle<StreamStateHandle> legacyOperatorState = subtaskOperatorState.getLegacyOperatorState();
					nonPartitionableState.add(legacyOperatorState.getLength() > 0 ? legacyOperatorState.get(0) : null);

					ChainedStateHandle<OperatorStateHandle> managedOperatorState = subtaskOperatorState.getManagedOperatorState();
					operatorStateFromBackend.add(Collections.singletonList(managedOperatorState != null ? managedOperatorState.get(0) : null));

					ChainedStateHandle<OperatorStateHandle> rawOperatorState = subtaskOperatorState.getRawOperatorState();
					operatorStateFromStream.add(Collections.singletonList(rawOperatorState != null ? rawOperatorState.get(0) : null));
				}

				TaskStateHandles taskStateHandles = new TaskStateHandles(
					new ChainedStateHandle<>(nonPartitionableState),
					operatorStateFromBackend,
					operatorStateFromStream,
					Collections.singletonList(newKeyedStatesBackend),
					Collections.singletonList(newKeyedStateStream));

				currentExecutionAttempt.setInitialState(taskStateHandles);
			}
		}

		return true;
	}
}
