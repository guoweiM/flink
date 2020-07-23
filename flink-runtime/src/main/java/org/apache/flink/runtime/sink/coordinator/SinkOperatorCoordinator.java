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

package org.apache.flink.runtime.sink.coordinator;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkManager;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.sink.event.SinkEventWrapper;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public class SinkOperatorCoordinator<CheckpointT> implements OperatorCoordinator {

	private SinkManager<CheckpointT> sinkManager;

	private final Sink<?, CheckpointT> sink;

	private final SimpleVersionedSerializer<CheckpointT> checkpointSimpleVersionedSerializer;

	public SinkOperatorCoordinator(Sink<?, CheckpointT> sink, SimpleVersionedSerializer<CheckpointT> checkpointSimpleVersionedSerializer) {
		this.sink = sink;
		this.checkpointSimpleVersionedSerializer = checkpointSimpleVersionedSerializer;
	}

	@Override
	public void start() {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {

		if (event instanceof SinkEventWrapper) {
			SinkEventWrapper sinkEventWrapper = (SinkEventWrapper) event;
			sinkManager.handleSinkEvent(subtask, sinkEventWrapper.getSinkEvent());
		} else {
			//TODO:: throw sth.
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		sinkManager.failTask(subtask);
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
		final CheckpointT checkpoint = sinkManager.checkpoint(checkpointId);
		resultFuture.complete(checkpointSimpleVersionedSerializer.serialize(checkpoint));
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		sinkManager.checkpointComplete(checkpointId);
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		final CheckpointT checkpoint = SimpleVersionedSerialization.readVersionAndDeSerialize(checkpointSimpleVersionedSerializer, checkpointData);
		sinkManager = sink.restoreSinkManager(checkpoint);
	}
}
