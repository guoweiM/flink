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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

@Internal
abstract class AbstractWriterOperator<InputT, CommT> extends AbstractStreamOperator<CommT>
	implements OneInputStreamOperator<InputT, CommT>, BoundedOneInput {

	private static final long serialVersionUID = 1L;

	// ------------------------------- runtime fields ---------------------------------------

	private transient Long currentWatermark;

	private transient StreamRecord<CommT> streamRecord;

	private transient Writer<InputT, CommT, ?> writer;

	@Override
	public void open() throws Exception {
		super.open();

		this.currentWatermark = Long.MIN_VALUE;

		this.streamRecord = new StreamRecord<>(null);

		writer = createWriter();
	}

	@Override
	public void processElement(StreamRecord<InputT> element) throws Exception {
		writer.write(element.getValue(), new Writer.Context() {
			@Override
			public long currentWatermark() {
				return currentWatermark;
			}

			@Override
			public Long timestamp() {
				return element.hasTimestamp() ? element.getTimestamp() : null;
			}
		});
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		sendCommittables(writer.prepareCommit(false));
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public void endInput() throws Exception {
		sendCommittables(writer.prepareCommit(true));
	}

	@Override
	public void close() throws Exception {
		super.close();
		writer.close();
	}

	protected Sink.InitContext createInitContext() {
		return new Sink.InitContext() {
			@Override
			public int getSubtaskId() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public MetricGroup metricGroup() {
				return getMetricGroup();
			}
		};
	}

	protected Writer<InputT, CommT, ?> getWriter() {
		return writer;
	}

	abstract Writer<InputT, CommT, ?> createWriter() throws Exception;

	private void sendCommittables(final List<CommT> committables) {
		for (CommT committable : committables) {
			streamRecord.replace(committable);
			output.collect(streamRecord);
		}
	}
}
