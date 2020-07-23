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

package org.apache.flink.streaming.api.functions.splitsink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink.SinkWriterContext;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SplitSinkWriter<IN, SplitT> implements SinkWriter<IN> {

	private static final ListStateDescriptor<byte[]> SINK_SPLITS_STATE_DES =
		new ListStateDescriptor<>("sink-splits", BytePrimitiveArraySerializer.INSTANCE);

	private final SplitWriter<IN, SplitT> splitWriter;

	private final SplitCommitter<SplitT> splitCommitter;

	private final SimpleVersionedSerializer<SplitT> splitSimpleVersionedSerializer;

	private final NavigableMap<Long, List<SplitT>> splitsPerCheckpoint = new TreeMap<>();

	private final ListState<byte[]> splitsState;

	private final SinkWriterContext sinkWriterContext;

	public SplitSinkWriter(
		boolean isRestore,
		SplitWriter<IN, SplitT> splitWriter,
		SplitCommitter<SplitT> splitCommitter,
		SimpleVersionedSerializer<SplitT> splitSimpleVersionedSerializer,
		SinkWriterContext sinkWriterContext) throws Exception {

		this.splitWriter = splitWriter;
		this.splitCommitter = splitCommitter;
		this.splitSimpleVersionedSerializer = splitSimpleVersionedSerializer;
		this.splitsState = sinkWriterContext.getOperatorStateStore().getListState(SINK_SPLITS_STATE_DES);
		this.sinkWriterContext = sinkWriterContext;

		if (isRestore) {
			final List<SplitT> allRestoredSplits = new ArrayList<>();
			for (byte[] s : splitsState.get()) {
				final SplitT split = SimpleVersionedSerialization.readVersionAndDeSerialize(splitSimpleVersionedSerializer, s);
				allRestoredSplits.add(split);
			}
			splitCommitter.commit(allRestoredSplits);
		}
	}

	@Override
	public void write(IN in, Context context) throws Exception {
		splitWriter.write(in, context);
	}

	@Override
	public void preCommit(long checkpointId) throws Exception {
		final List<SplitT> allSplits = persist(checkpointId);
		splitsState.clear();
		for (SplitT split : allSplits) {
			//TODO:: fix the serializer
			// splitsState.add(splitSimpleVersionedSerializer.serialize(split));
			break;
		}
		return;
	}

	@Override
	public void commitUpTo(long checkpointId) throws IOException {

		Iterator<Map.Entry<Long, List<SplitT>>> it =
			splitsPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<SplitT>> item = it.next();
			splitCommitter.commit(item.getValue());
			it.remove();
		}
	}

	@Override
	public void flush() throws Exception {
		splitWriter.flush();
		List<SplitT> splits = persist(Long.MAX_VALUE);
		sinkWriterContext.sendSinkEventToSinkManager(new FinalSplitsEvent<>(splits));
	}

	private List<SplitT> persist(long checkpointId) throws Exception {
		final List<SplitT> splits = splitWriter.preCommit();
		final List<SplitT> allSplits = new ArrayList<>();
		splitsPerCheckpoint.put(checkpointId, splits);

		for (List<SplitT> splitList : splitsPerCheckpoint.values()) {
			allSplits.addAll(splitList);
		}
		splitWriter.persist();

		return allSplits;
	}
}
