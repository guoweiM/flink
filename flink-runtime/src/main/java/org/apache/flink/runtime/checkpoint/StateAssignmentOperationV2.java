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
import org.apache.flink.runtime.state.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

	


	private Map<JobVertexID, List<Collection<OperatorStateHandle>>> repartitionOperatorStates(ExecutionJobVertex executionJobVertex,
		Map<JobVertexID, List<OperatorStateHandle>> allOpStates,
		OperatorStateRepartitioner opStateRepartitioner){

		Map<JobVertexID, List<Collection<OperatorStateHandle>>> repartitionedOpStates = new HashMap<>();

		int newParallelism = executionJobVertex.getParallelism();

		JobVertexID[] operatorIDs = executionJobVertex.getOperatorIDs();

		for(JobVertexID operatorID : operatorIDs){
			List<OperatorStateHandle> operatorStates = allOpStates.get(operatorID);
			int oldParallelism = allOpStates.get(operatorID).size();

			repartitionedOpStates.put(operatorID,
				StateAssignmentOperation.applyRepartitioner(opStateRepartitioner,
					operatorStates, oldParallelism, newParallelism));
		}

		return repartitionedOpStates;

	}

	private void repartitionKeyedStates(ExecutionJobVertex executionJobVertex,
		List<KeyedStateHandle> keyedStatesBackend,
		List<KeyedStateHandle> keyedStatesStream,
		List<Collection<KeyedStateHandle>> newKeyedStatesBackend,
		List<Collection<KeyedStateHandle>> newKeyedStatesStream){

		int newParallelism = executionJobVertex.getParallelism();
		int oldParallelism;

		if (keyedStatesBackend == null && keyedStatesStream == null){
			return;
		}

		if (keyedStatesBackend != null && keyedStatesStream != null){
			if (keyedStatesBackend.size() != keyedStatesStream.size()){
				throw new IllegalArgumentException("The number of keyedStatesBackend(" +
					+ keyedStatesBackend.size()+ ") is different with the number of keyedStatesStream(" +
					+ keyedStatesStream.size());
			}
		}
		if (keyedStatesBackend != null){
			oldParallelism = keyedStatesBackend.size();
		}else{
			oldParallelism = keyedStatesStream.size();
		}

		if (newParallelism == oldParallelism){
			for(int i=0; i < newParallelism; i++) {
				KeyedStateHandle keyedState = keyedStatesBackend != null ? keyedStatesBackend.get(i) : null;
				KeyedStateHandle keyedStream = keyedStatesStream != null ? keyedStatesStream.get(i) : null;

				newKeyedStatesBackend.add(keyedState == null ? null : Collections.singletonList(keyedState));
				newKeyedStatesStream.add(keyedStream == null ? null : Collections.singletonList(keyedStream));
			}
		}else{
			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
				executionJobVertex.getMaxParallelism(),
				newParallelism);

			for(int i = 0; i < newParallelism; i++){
			}
		}
	}


	/**
	 * This method collects states which is needed by the {@param executionJobVertex}.
	 * All the states which are needed by {@param executionJobVertex} are stored to five different maps according to the
	 * state type.
	 *
	 * @param executionJobVertex      the vertex which is needed to collect states.
	 *
	 * @param allLegacyOperatorStates all the legacy states needed by {@param executionJobVertex}
	 * @param allOpStatesBackend      all the managed operator states needed by {@param executionJobVertex}
	 * @param allOpStatesStream       all the raw operator states needed by {@param executionJobVertex}
	 * @param allKeyedStatesBackend   all the managed keyed states needed by {@param executionJobVertex}
	 * @param allKeyedStatesStream    all the raw keyed states needed by {@param executionJobVertex}
	 */

	private void collectStates(ExecutionJobVertex executionJobVertex,
		Map<JobVertexID, List<StreamStateHandle>> allLegacyOperatorStates,
		Map<JobVertexID, List<OperatorStateHandle>> allOpStatesBackend,
		Map<JobVertexID, List<OperatorStateHandle>> allOpStatesStream,
		Map<JobVertexID, List<KeyedStateHandle>> allKeyedStatesBackend,
		Map<JobVertexID, List<KeyedStateHandle>> allKeyedStatesStream){

		JobVertexID[] operatorIDs = executionJobVertex.getOperatorIDs();

		for(JobVertexID operatorID : operatorIDs){
			//find the operator state
			TaskState operatorState = this.taskStates.get(operatorID);

			if (operatorState != null){
				List<StreamStateHandle> legacyOperatorStates = new ArrayList<>();

				List<OperatorStateHandle> opStatesBackend = new ArrayList<>();
				List<OperatorStateHandle> opStatesStream = new ArrayList<>();

				List<KeyedStateHandle> keyedStatesBackend = new ArrayList<>();
				List<KeyedStateHandle> keyedStatesStream = new ArrayList<>();

				allLegacyOperatorStates.put(operatorID, legacyOperatorStates);

				allOpStatesBackend.put(operatorID, opStatesBackend);
				allOpStatesStream.put(operatorID, opStatesStream);

				allKeyedStatesBackend.put(operatorID, keyedStatesBackend);
				allKeyedStatesStream.put(operatorID, keyedStatesStream);

				for(int i = 0; i < operatorState.getParallelism(); i++){
					SubtaskState subtaskState = operatorState.getState(i);
					legacyOperatorStates.add(subtaskState.getLegacyOperatorState().get(0));

					if (subtaskState.getManagedOperatorState() != null && subtaskState.getManagedOperatorState().getLength() != 0){
						if (subtaskState.getManagedOperatorState().getLength() == 1){
							opStatesBackend.add(subtaskState.getManagedOperatorState().get(0));
						}else{
							throw new IllegalStateException("("+operatorID+") Length of the managed operator states is more than 1");
						}
					}else{
						opStatesBackend.add(null);
					}

					if (subtaskState.getRawKeyedState() != null && subtaskState.getRawOperatorState().getLength() != 0){
						if (subtaskState.getRawOperatorState().getLength() == 1){
							opStatesStream.add(subtaskState.getRawOperatorState().get(0));
						}else{
							throw new IllegalStateException("("+operatorID+") Length of the raw operator states is more than 1");
						}
					}else{
						opStatesStream.add(null);
					}
					keyedStatesBackend.add(subtaskState.getManagedKeyedState());
					keyedStatesStream.add(subtaskState.getRawKeyedState());
				}
			}
		}
	}


}
