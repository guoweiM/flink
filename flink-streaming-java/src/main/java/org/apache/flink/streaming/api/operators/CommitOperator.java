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
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing a {@link org.apache.flink.api.dag.CommitTransformation}.
 */
@Internal
public class CommitOperator<CommitT>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<CommitT, Void>, BoundedOneInput {

	private static final long serialVersionUID = 1L;

	private final CommitFunction<CommitT> commitFunction;
	private final TypeSerializer<CommitT> commitSerializer;

	private transient ListState<CommitT> commits;

	public CommitOperator(
			CommitFunction<CommitT> commitFunction,
			TypeSerializer<CommitT> commitSerializer) {
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
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
	}

	@Override
	public void endInput() throws Exception {
		// potentially we should not commit right away here but set a flag and then commit
		// once we get the next or final checkpoint complete notification
		for (CommitT commit : commits.get()) {
			commitFunction.commit(commit);
		}
	}

	@Override
	public void processElement(StreamRecord<CommitT> element) throws Exception {
		commits.add(element.getValue());
	}
}
