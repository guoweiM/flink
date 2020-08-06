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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink.SplitSink;
import org.apache.flink.api.connector.sink.SplitSinkWriter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * //TODO java doc.
 * @param <IN>
 * @param <SplitT>
 */
public class SplitSinkOperator<IN, SplitT> extends AbstractStreamOperator<SplitT> implements OneInputStreamOperator<IN, SplitT> , BoundedOneInput {

	private final SplitSink<IN, SplitT> splitSink;

	private transient SplitSinkWriter<IN, SplitT> splitSinkWriter = null;

	private transient SplitCollector<SplitT> collector;

	public SplitSinkOperator(SplitSink<IN, SplitT> splitSink) {
		this.splitSink = splitSink;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		collector = new SplitCollector<>(output);
		splitSinkWriter = splitSink.createWriter();
		splitSinkWriter.open(new SplitSinkWriter.InitialContext<SplitT>() {
			@Override
			public boolean isRestored() {
				return context.isRestored();
			}

			@Override
			public int getSubtaskIndex() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public Collector<SplitT> getCollector() {
				return collector;
			}

			@Override
			public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
				return context.getOperatorStateStore().getListState(stateDescriptor);
			}

			@Override
			public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
				return context.getOperatorStateStore().getUnionListState(stateDescriptor);
			}
		});
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		//TODO :: maybe we should not expose persist to the developer.
		splitSinkWriter.persistent();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		splitSinkWriter.write(element.getValue(), new SplitSinkWriter.Context() {
			@Override
			public long currentProcessingTime() {
				throw new RuntimeException("TODO.");
			}

			@Override
			public long currentWatermark() {
				throw new RuntimeException("TODO.");
			}

			@Override
			public Long timestamp() {
				return element.getTimestamp();
			}
		});
	}

	@Override
	public void endInput() throws IOException {
		splitSinkWriter.flush();
	}

	@Override
	public void close() throws Exception {
		super.close();
		splitSinkWriter.close();
		collector.close();
	}

	class SplitCollector<SplitT> implements Collector<SplitT> {

		private final Output<StreamRecord<SplitT>> output;

		private final StreamRecord<SplitT> streamRecord;

		public SplitCollector(Output<StreamRecord<SplitT>> output) {
			this.output = output;
			this.streamRecord = new StreamRecord<>(null);
		}

		@Override
		public void collect(SplitT split) {
			output.collect(streamRecord.replace(split));
		}

		@Override
		public void close() {

		}
	}
}
