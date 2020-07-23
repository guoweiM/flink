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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink.SplitCommitter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * There are two options:
 * 1. We could use a UnifiedSinkOperator to support both pipeline mode and batch mode.
 * 2. We could use a PipelineSinkOperator to support pipeline mode and use the BatchSinkOperator for the batch mode.
 */
class UnifiedSinkOperator<IN, SplitT> extends AbstractStreamOperator<Object> implements
	OneInputStreamOperator<IN, Object>, CheckpointListener, BoundedOneInput {

	private static final ListStateDescriptor<byte[]> SINK_SPLITS_STATE_DES =
		new ListStateDescriptor<>("sink-splits", BytePrimitiveArraySerializer.INSTANCE);

	private final Sink<IN, SplitT> sink;

	private final SimpleVersionedSerializer<SplitT> splitSerializer;

	private final SplitCommitter<SplitT> splitCommitter;

	private final NavigableMap<Long, List<SplitT>> splitsPerCheckpoint;

	private final OperatorEventGateway operatorEventGateway;

	private UnifiedSinkInitialContext unifiedSinkInitialContext;

	private Long currentWaterMark = Long.MIN_VALUE;

	private SinkWriter<IN, SplitT> sinkWriter;

	private ListState<byte[]> sinkSplitsState;

	public UnifiedSinkOperator(Sink<IN, SplitT> sink, OperatorEventGateway operatorEventGateway) throws Exception {
		this.sink = sink;
		this.splitSerializer = sink.getSplitSerializer();
		this.operatorEventGateway = operatorEventGateway;
		this.splitsPerCheckpoint = new TreeMap<>();
		this.splitCommitter = sink.createSplitCommitter();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		unifiedSinkInitialContext =
			new UnifiedSinkInitialContext(context.getOperatorStateStore(), context.isRestored(), getRuntimeContext().getIndexOfThisSubtask());
		sinkSplitsState = context.getOperatorStateStore().getListState(SINK_SPLITS_STATE_DES);

		// restore and commit
		if (context.isRestored()) {
			final List<SplitT> allSplits = new ArrayList<>();
			for (byte[] sb : sinkSplitsState.get()) {
				SplitT split = SimpleVersionedSerialization.readVersionAndDeSerialize(splitSerializer, sb);
				allSplits.add(split);
			}
			splitCommitter.commit(allSplits);
		}
	}

	@Override
	public void open() throws Exception {
		sinkWriter = sink.createWriter(unifiedSinkInitialContext);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		sinkWriter.write(element.getValue(), new SinkWriter.Context() {
			@Override
			public long currentProcessingTime() {
				return getRuntimeContext().getProcessingTimeService().getCurrentProcessingTime();
			}

			@Override
			public long currentWatermark() {
				return currentWaterMark;
			}

			@Override
			public Long timestamp() {
				return element.getTimestamp();
			}
		});
	}

	@Override
	public void processWatermark(Watermark watermark) throws Exception {
		super.processWatermark(watermark);
		this.currentWaterMark = watermark.getTimestamp();
	}

	@Override
	public void snapshotState(StateSnapshotContext stateSnapshotContext) throws Exception {

		sinkSplitsState.clear();

		for (SplitT split : persist(stateSnapshotContext.getCheckpointId())) {
				sinkSplitsState.add(splitSerializer.serialize(split));
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws IOException {
		final Iterator<Map.Entry<Long, List<SplitT>>> it =
			splitsPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<SplitT>> entry = it.next();
			splitCommitter.commit(entry.getValue());
			it.remove();
		}
	}

	@Override
	public void endInput() throws Exception {
		// 1. notify the writer at the end of stream
		sinkWriter.flush();

		// 2. send all the splits to the sink operator coordinator
		operatorEventGateway.sendEventToCoordinator(new UnifiedSinkCoordinator.LastSinkSplitsEvent<>(persist(Long.MAX_VALUE), splitSerializer));

	}

	private List<SplitT> persist(long checkpointId) throws Exception {
		final List<SplitT> allSplits = new ArrayList<>();

		// 1. Get the committable data from the writer
		final List<SplitT> splits = sinkWriter.preCommit();

		// 2. Record the splits of per checkpoint
		splitsPerCheckpoint.put(checkpointId, splits);

		// 3. Collect all the splits
		for (List<SplitT> splitList : splitsPerCheckpoint.values()) {
			allSplits.addAll(splitList);
		}

		// 4. persist the writer's state
		sinkWriter.persist();

		return allSplits;
	}

	class UnifiedSinkInitialContext implements SinkInitialContext {

		private final OperatorStateStore operatorStateStore;

		private final boolean isRestored;

		private final int subtaskIndex;

		UnifiedSinkInitialContext(OperatorStateStore operatorStateStore, boolean isRestored, int subtaskIndex) {
			this.operatorStateStore = operatorStateStore;
			this.isRestored = isRestored;
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public ProcessingTimeService getProcessingTimeService() {
			return getRuntimeContext().getProcessingTimeService();
		}

		@Override
		public OperatorStateStore getOperatorStateStore() {
			return operatorStateStore;
		}

		@Override
		public boolean isRestored() {
			return isRestored;
		}

		@Override
		public int getSubtaskIndex() {
			return subtaskIndex;
		}
	}
}
