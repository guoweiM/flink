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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.connector.sink.SplitCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class UnifiedSinkCoordinator<SplitT> implements OperatorCoordinator {

	private Map<Integer, List<SplitT>> finalSplitsFromSubtask = new HashMap();

	private final SplitCommitter<SplitT> splitCommitter;

	private final SimpleVersionedSerializer<SplitT> splitSimpleVersionedSerializer;

	public UnifiedSinkCoordinator(SplitCommitter<SplitT> splitCommitter, SimpleVersionedSerializer<SplitT> splitSimpleVersionedSerializer) {
		this.splitCommitter = splitCommitter;
		this.splitSimpleVersionedSerializer = splitSimpleVersionedSerializer;
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {

		if (event instanceof UnifiedSinkCoordinator.LastSinkSplitsEvent) {
			final LastSinkSplitsEvent<SplitT> lastSinkSplitsEvent = (LastSinkSplitsEvent<SplitT>) event;
			final List<SplitT> splits = lastSinkSplitsEvent.splits(splitSimpleVersionedSerializer);
			finalSplitsFromSubtask.put(subtask, splits);
		} else {
			//TODO::
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		finalSplitsFromSubtask.remove(subtask);
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
		byte[] result = new byte[1000];
		//we assume that the checkpoint is last one
		for (List<SplitT> splitList : finalSplitsFromSubtask.values()) {
			for (SplitT split : splitList) {
				byte [] s = splitSimpleVersionedSerializer.serialize(split);
				//TODO: copy s to result;
			}
		}
		resultFuture.complete(result);
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		// we assume that the checkpoint is the last one
		for (List<SplitT> splitList : finalSplitsFromSubtask.values()) {
			try {
				splitCommitter.commit(splitList);
			} catch (IOException e) {
				e.printStackTrace();
				//todo fail the job
			}
		}
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		// 1. restore all the split
		// 2. commit it
	}

	public static class LastSinkSplitsEvent<SplitT> implements OperatorEvent  {
		private static final long serialVersionUID = 1L;
		private final int serializerVersion;
		private final ArrayList<byte[]> splits;

		public LastSinkSplitsEvent(List<SplitT> splits, SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
			this.splits = new ArrayList<>(splits.size());
			this.serializerVersion = splitSerializer.getVersion();
			for (SplitT split : splits) {
				this.splits.add(splitSerializer.serialize(split));
			}
		}

		public List<SplitT> splits(SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
			List<SplitT> result = new ArrayList<>(splits.size());
			for (byte[] serializedSplit : splits) {
				result.add(splitSerializer.deserialize(serializerVersion, serializedSplit));
			}
			return result;
		}
	}
}
