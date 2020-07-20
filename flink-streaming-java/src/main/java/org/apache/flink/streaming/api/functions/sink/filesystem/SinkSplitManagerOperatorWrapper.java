/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.SplitCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;

class SinkSplitManagerOperatorWrapper<SplitT> implements AutoCloseable {

	private static final ListStateDescriptor<byte[]> SINK_SPLITS_STATE_DES = new ListStateDescriptor<>("sink-splits", BytePrimitiveArraySerializer.INSTANCE);

	private final SinkSplitManager<SplitT> sinkSplitManager;

	private final ListState<byte[]> sinkSplitsState;

	private final SimpleVersionedSerializer<SplitT> splitSerializer;

	public SinkSplitManagerOperatorWrapper(
		boolean isRestore,
		OperatorStateStore operatorStateStore,
		SplitCommitter sinkSplitCommitter,
		SimpleVersionedSerializer<SplitT> splitSerializer) throws Exception {

		this.sinkSplitManager = new SinkSplitManager<>(sinkSplitCommitter);
		this.sinkSplitsState = operatorStateStore.getListState(SINK_SPLITS_STATE_DES);
		this.splitSerializer = splitSerializer;

		if (isRestore) {

		}
	}

	void add(int subtaskId, List<SplitT> splits) {
		sinkSplitManager.add(subtaskId, splits);
	}

	void persist(long checkpointId) throws Exception {
		final List<SplitT> splits = sinkSplitManager.persist(checkpointId);
		sinkSplitsState.clear();
		for (SplitT split : splits) {
			sinkSplitsState.add(splitSerializer.serialize(split));
		}
	}

	void checkpointComplete(long checkpointId) throws IOException {
		sinkSplitManager.checkpointComplete(checkpointId);
	}

	@Override
	public void close() throws Exception {
		sinkSplitManager.close();
	}
}
