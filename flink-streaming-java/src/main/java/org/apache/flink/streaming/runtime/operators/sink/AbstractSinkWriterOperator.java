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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract base class for operators that work with a {@link SinkWriter}.
 *
 * <p>Sub-classes are responsible for creating the specific {@link SinkWriter} by implementing {@link
 * #createWriter()}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 */
@Internal
abstract class AbstractSinkWriterOperator<InputT, CommT> extends AbstractStreamOperator<CommT>
		implements OneInputStreamOperator<InputT, CommT>, BoundedOneInput, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	/** The runtime information of the input element. */
	private final Context<InputT> context;

	/** User can use to register a processing timer when calling {@link SinkWriter#onTimer(long, SinkWriter.OnTimerContext)}. */
	private final OnTimerContext onTimerContext;

	private final ProcessingTimeService processingTimeService;

	// ------------------------------- runtime fields ---------------------------------------

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private Long currentWatermark;

	/** The sink writer that does most of the work. */
	protected SinkWriter<InputT, CommT, ?> sinkWriter;

	AbstractSinkWriterOperator(ProcessingTimeService processingTimeService) {
		this.processingTimeService = checkNotNull(processingTimeService);
		this.context = new Context<>();
		this.onTimerContext = new OnTimerContext();
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.currentWatermark = Long.MIN_VALUE;

		sinkWriter = createWriter();
	}

	@Override
	public void processElement(StreamRecord<InputT> element) throws Exception {
		context.element = element;
		sinkWriter.write(element.getValue(), context);
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		sendCommittables(sinkWriter.prepareCommit(false));
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public void endInput() throws Exception {
		sendCommittables(sinkWriter.prepareCommit(true));
	}

	@Override
	public void close() throws Exception {
		super.close();
		sinkWriter.close();
	}

	public void onProcessingTime(long timestamp) throws Exception {
		sinkWriter.onTimer(timestamp, onTimerContext);
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

	/**
	 * Creates and returns a {@link SinkWriter}.
	 *
	 * @throws Exception If creating {@link SinkWriter} fail
	 */
	abstract SinkWriter<InputT, CommT, ?> createWriter() throws Exception;

	private void sendCommittables(final List<CommT> committables) {
		for (CommT committable : committables) {
			output.collect(new StreamRecord<>(committable));
		}
	}

	private class Context<IN> implements SinkWriter.Context {

		private StreamRecord<IN> element;

		@Override
		public void registerProcessingTimer(long time) {
			processingTimeService.registerTimer(time, AbstractSinkWriterOperator.this);
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public Long timestamp() {
			if (element.hasTimestamp()) {
				return element.getTimestamp();
			}
			return null;
		}
	}

	private class OnTimerContext implements SinkWriter.OnTimerContext {

		@Override
		public void registerProcessingTimer(long time) {
			processingTimeService.registerTimer(time, AbstractSinkWriterOperator.this);
		}
	}
}
