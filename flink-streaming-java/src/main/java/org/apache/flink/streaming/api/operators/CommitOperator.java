/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;

/**
 * A {@link StreamOperator} for executing a {@link org.apache.flink.api.dag.CommitTransformation}.
 */
@Internal
public class CommitOperator<CommitT>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<CommitT, Void>, BoundedOneInput , CheckpointedFunction, CheckpointListener {

	private static final long serialVersionUID = 1L;

	private final CommitFunction<CommitT> commitFunction;
	private final TypeSerializer<CommitT> commitSerializer;

	private transient ListState<CommitT> commits;

	private final OperatorEventGateway operatorEventGateway;

	private final NavigableMap<Long, List<CommitT>> splitsPerCheckpoint = new TreeMap<>();

	private List<CommitT> currentSplits;

	public CommitOperator(
			CommitFunction<CommitT> commitFunction,
			TypeSerializer<CommitT> commitSerializer,
			OperatorEventGateway operatorEventGateway) {
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
		this.operatorEventGateway = operatorEventGateway;
		this.currentSplits = new ArrayList<>();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		ListStateDescriptor<CommitT> commitStateDescriptor = new ListStateDescriptor<>(
				"commits",
				commitSerializer);

		// I don't like using operator state here, if this goes past POC stage we need to
		// have something better
		this.commits = context.getOperatorStateStore().getListState(commitStateDescriptor);

		if (context.isRestored()) {
			for(CommitT commit : commits.get()) {
				commitFunction.commit(commit);
			}
		}
	}

	@Override
	public void endInput() throws Exception {
		// potentially we should not commit right away here but set a flag and then commit
		// once we get the next or final checkpoint complete notification
		for (CommitT commit : commits.get()) {
			commitFunction.commit(commit);
		}



		/** we could send to operator coordinator the like following **/


		/**
			List<CommitT> allCommits = new ArrayList<>();
			for (List<CommitT> commitList : splitsPerCheckpoint.values()) {
				allCommits.addAll(commitList);
			}
			FinalSplitsEvent finalSplitsEvent = new FinalSplitsEvent(allCommits);
			operatorEventGateway.sendEventToCoordinator(finalSplitsEvent);
		 **/

	}

	@Override
	public void processElement(StreamRecord<CommitT> element) throws Exception {
		currentSplits.add(element.getValue());
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		splitsPerCheckpoint.put(context.getCheckpointId(), currentSplits);
		currentSplits = new ArrayList<>();

		// TODO snap shot all the splitsPerCheckpoint
		return;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		Iterator<Map.Entry<Long, List<CommitT>>> it =
			splitsPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<CommitT>> item = it.next();
			for(CommitT commit : item.getValue()) {
				commitFunction.commit(commit);
			}
			it.remove();
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {

	}
}
