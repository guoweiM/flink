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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.migration.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
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
				//TODO:: check operatorState chainLength is 1
				operatorStates.add(operatorState);
			}

			assignAttemptState(task.getValue(), operatorStates);
		}

		return true;
	}



	private void assignAttemptState(ExecutionJobVertex executionJobVertex, List<TaskState> operatorStates){

		//TODO:: check the sequence of the operator ids.
		JobVertexID[] operatorIDs = executionJobVertex.getOperatorIDs();

		int newParallelism = executionJobVertex.getParallelism();

		List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
			executionJobVertex.getMaxParallelism(),
			newParallelism);

		/**
		 *
		 * ReDistribute ManagedOperatorStates and RawOperatorStates from old parallelism to new parallelism.
		 *
		 * The old ManagedOperatorStates with old parallelism 3:
		 *
		 * 		parallelism0 parallelism1 parallelism2
		 * op0   states0,0    state0,1	   state0,2
		 * op1
		 * op2   states2,0    state2,1	   state1,2
		 * op3   states3,0    state3,1     state3,2
		 *
		 * The new ManagedOperatorStates with new parallelism 4:
		 *
		 * 		parallelism0 parallelism1 parallelism2 parallelism3
		 * op0   state0,0	  state0,1 	   state0,2		state0,3
		 * op1
		 * op2   state2,0	  state2,1 	   state2,2		state2,3
		 * op3   state3,0	  state3,1 	   state3,2		state3,3
		 */
		List<List<Collection<OperatorStateHandle>>> newManagedOperatorStates = new ArrayList<>();
		List<List<Collection<OperatorStateHandle>>> newRawOperatorStates = new ArrayList<>();

		reDistributePartitionableStates(operatorStates, newParallelism, newManagedOperatorStates, newRawOperatorStates);


		// compute the state by task
		for(int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

			Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[subTaskIndex]
				.getCurrentExecutionAttempt();

			List<StreamStateHandle> subNonPartitionableState = new ArrayList<>();

			Tuple2<Collection<KeyedStateHandle>, Collection<KeyedStateHandle>> subKeyedState = null;

			List<Collection<OperatorStateHandle>> subManagedOperatorState = new ArrayList<>();
			List<Collection<OperatorStateHandle>> subRawOperatorState = new ArrayList<>();



			for(int operatorIndex = 0; operatorIndex < operatorIDs.length; operatorIndex++) {
				//TODO:: check operatorState chain is 1 before this method
				TaskState operatorState = operatorStates.get(operatorIndex);
				int oldParallelism = operatorState.getParallelism();

				// NonPartitioned State
				reassignSubNonPartitionedStates(operatorIDs[operatorIndex],
					operatorState,
					subTaskIndex,
					newParallelism,
					oldParallelism,
					subNonPartitionableState);

				// PartitionedState
				reAssignSubPartitionableState(newManagedOperatorStates,
					newRawOperatorStates,
					subTaskIndex,
					operatorIndex,
					subManagedOperatorState,
					subRawOperatorState);

				// KeyedState
				if (operatorIndex == operatorIDs.length - 1 ) {
					subKeyedState = reassignSubKeyedStates(operatorState,
						keyGroupPartitions,
						subTaskIndex,
						newParallelism,
						oldParallelism);
				}
			}

			TaskStateHandles taskStateHandles = new TaskStateHandles(
				new ChainedStateHandle<>(subNonPartitionableState),
				subManagedOperatorState,
				subRawOperatorState,
				subKeyedState != null ? subKeyedState.f0 : null,
				subKeyedState != null ? subKeyedState.f1 : null);

			currentExecutionAttempt.setInitialState(taskStateHandles);
		}
	}




	private void reAssignSubPartitionableState(
		List<List<Collection<OperatorStateHandle>>> newMangedOperatorStates,
		List<List<Collection<OperatorStateHandle>>> newRawOperatorStates,
		int subTaskIndex, int operatorIndex,
		List<Collection<OperatorStateHandle>> subManagedOperatorState,
		List<Collection<OperatorStateHandle>> subRawOperatorState){

		if (newMangedOperatorStates.get(operatorIndex) != null) {
			subManagedOperatorState.add(newMangedOperatorStates.get(operatorIndex).get(subTaskIndex));
		}else{
			subManagedOperatorState.add(null);
		}
		if (newRawOperatorStates.get(operatorIndex) != null) {
			subRawOperatorState.add(newRawOperatorStates.get(operatorIndex).get(subTaskIndex));
		}else{
			subRawOperatorState.add(null);
		}


	}

	private Tuple2<Collection<KeyedStateHandle>,Collection<KeyedStateHandle>> reassignSubKeyedStates(
		TaskState operatorState,
		List<KeyGroupRange> keyGroupPartitions,
		int subTaskIndex,
		int newParallelism,
		int oldParallelism){

		Collection<KeyedStateHandle> subManagedKeyedState;
		Collection<KeyedStateHandle> subRawKeyedState;

		if (newParallelism == oldParallelism) {
			if (operatorState.getState(subTaskIndex) != null){
				KeyedStateHandle oldSubManagedKeyedState = operatorState.getState(subTaskIndex).getManagedKeyedState();
				KeyedStateHandle oldSubRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState();
				subManagedKeyedState = oldSubManagedKeyedState != null ? Collections.singletonList(
					oldSubManagedKeyedState) : null;
				subRawKeyedState = oldSubRawKeyedState != null ? Collections.singletonList(
					oldSubRawKeyedState) : null;
			}else{
				subManagedKeyedState = null;
				subRawKeyedState = null;
			}
		} else{
			subManagedKeyedState = getManagedKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
			subRawKeyedState = getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
		}
		return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
	}

	private void reassignSubNonPartitionedStates(JobVertexID operatorID,
		TaskState operatorState,
		int subTaskIndex,
		int newParallelism,
		int oldParallelism,
		List<StreamStateHandle> subNonPartitionableState){
		if (operatorState.hasNonPartitionedState() && (oldParallelism != newParallelism)) {
			throw new IllegalStateException(
				"Cannot restore the latest checkpoint because " + "the operator " + operatorID +
					" has non-partitioned " + "state and its parallelism changed. The operator " +
					operatorID + " has parallelism " + newParallelism + " whereas the corresponding " +
					"state object has a parallelism of " + oldParallelism);
		}

		if (oldParallelism == newParallelism) {
			if (operatorState.getState(subTaskIndex) != null &&
				!operatorState.getState(subTaskIndex).getLegacyOperatorState().isEmpty()) {
				subNonPartitionableState.add(operatorState.getState(subTaskIndex).getLegacyOperatorState().get(0));
			}else{
				subNonPartitionableState.add(null);
			}
		}else{
			subNonPartitionableState.add(null);
		}
	}

	private void reDistributePartitionableStates(List<TaskState> operatorStates, int newParallelism,
		List<List<Collection<OperatorStateHandle>>> newManagedOperatorStates,
		List<List<Collection<OperatorStateHandle>>> newRawOperatorStates){

		//collect the old partitionalbe state
		List<List<OperatorStateHandle>> oldManagedOperatorStates = new ArrayList<>();
		List<List<OperatorStateHandle>> oldRawOperatorStates = new ArrayList<>();

		collectPartionableStates(operatorStates,oldManagedOperatorStates,oldRawOperatorStates);


		//redistribute
		OperatorStateRepartitioner opStateRepartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		for(int operatorIndex = 0; operatorIndex < operatorStates.size(); operatorIndex++){
			int oldParallelism = operatorStates.get(operatorIndex).getParallelism();
			newManagedOperatorStates.add(StateAssignmentOperation.applyRepartitioner(opStateRepartitioner,
				oldManagedOperatorStates.get(operatorIndex), oldParallelism, newParallelism));
			newRawOperatorStates.add(StateAssignmentOperation.applyRepartitioner(opStateRepartitioner,
				oldRawOperatorStates.get(operatorIndex), oldParallelism, newParallelism));

		}
	}


	private void collectPartionableStates(List<TaskState> operatorStates,
		List<List<OperatorStateHandle>> managedOperatorStates,
		List<List<OperatorStateHandle>> rawOperatorStates){

		for(TaskState operatorState : operatorStates){
			List<OperatorStateHandle> managedOperatorState =  null;
			List<OperatorStateHandle> rawOperatorState = null;

			for(int i = 0; i < operatorState.getParallelism(); i++){
				SubtaskState subtaskState = operatorState.getState(i);
				if (subtaskState != null){
					if (subtaskState.getManagedOperatorState() != null &&
						subtaskState.getManagedOperatorState().getLength() > 0 &&
						subtaskState.getManagedOperatorState().get(0) != null){
						if (managedOperatorState == null){
							managedOperatorState = new ArrayList<>();
						}
						managedOperatorState.add(subtaskState.getManagedOperatorState().get(0));
					}
					if (subtaskState.getRawKeyedState() != null &&
						subtaskState.getRawOperatorState().getLength() >0 &&
						subtaskState.getRawOperatorState().get(0) != null){
						if (rawOperatorState == null){
							rawOperatorState = new ArrayList<>();
						}
						rawOperatorState.add(subtaskState.getRawOperatorState().get(0));
					}
				}

			}
			managedOperatorStates.add(managedOperatorState);
			rawOperatorStates.add(rawOperatorState);
		}
	}


	/**
	 * Collect {@link KeyGroupsStateHandle  managedKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 *
	 * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
	 *
	 */
	public static List<KeyedStateHandle> getManagedKeyedStateHandles(
		TaskState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = null;

		for (int i = 0; i < operatorState.getParallelism(); i++) {
			if (operatorState.getState(i) != null && operatorState.getState(i).getManagedKeyedState() != null) {
				KeyedStateHandle intersectedKeyedStateHandle = operatorState.getState(i).getManagedKeyedState().getIntersection(subtaskKeyGroupRange);

				if (intersectedKeyedStateHandle != null) {
					if (subtaskKeyedStateHandles == null){
						subtaskKeyedStateHandles = new ArrayList<>();
					}
					subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
				}
			}
		}

		return subtaskKeyedStateHandles;
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  rawKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 *
	 * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getRawKeyedStateHandles(
		TaskState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = null;

		for (int i = 0; i < operatorState.getParallelism(); i++) {
			if (operatorState.getState(i) != null && operatorState.getState(i).getRawKeyedState() != null) {
				KeyedStateHandle intersectedKeyedStateHandle = operatorState.getState(i).getRawKeyedState().getIntersection(subtaskKeyGroupRange);

				if (intersectedKeyedStateHandle != null) {
					if (subtaskKeyedStateHandles == null){
						subtaskKeyedStateHandles = new ArrayList<>();
					}
					subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
				}
			}
		}

		return subtaskKeyedStateHandles;
	}


}
