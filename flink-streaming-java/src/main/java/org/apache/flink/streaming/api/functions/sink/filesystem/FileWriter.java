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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileWriter<IN, BucketID> implements SinkWriter<IN, FileSinkSplit>, ProcessingTimeCallback {

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
		new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
		new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	final Buckets<IN, BucketID> buckets;

	private final ListState<byte[]> bucketStatesContainer;

	private final ListState<Long> maxPartCountersState;

	private final ProcessingTimeService processingTimeService;

	private final long bucketCheckInterval;

	public FileWriter(
		Sink.InitialContext context,
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval) throws Exception {

		final FileSink.FileSinkInitialContext fileSinkInitialContext = (FileSink.FileSinkInitialContext) context;

		this.buckets = bucketsBuilder.createBuckets(context.getSubTaskIndex());
		this.processingTimeService = fileSinkInitialContext.getProcessingTimeService();
		this.bucketCheckInterval = bucketCheckInterval;

		this.bucketStatesContainer = context.getOperatorStateStore().getListState(BUCKET_STATE_DESC);
		this.maxPartCountersState = context.getOperatorStateStore().getUnionListState(MAX_PART_COUNTER_STATE_DESC);
		if (context.isRestored()) {
			this.buckets.initializeState(bucketStatesContainer, maxPartCountersState);
		}

		final long currentTime = processingTimeService.getCurrentProcessingTime();
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}

	@Override
	public void write(IN element, Context context) throws Exception {
		buckets.onElement(element, context.currentProcessingTime(), context.timestamp(), context.currentWatermark());
	}

	@Override
	public List<FileSinkSplit> preCommit() throws IOException {
		return buckets.preCommit();
	}

	@Override
	public void persist() throws Exception {
		buckets.persist(bucketStatesContainer, maxPartCountersState);
	}

	@Override
	public void flush() {
		buckets.flush();
	}

	@Override
	public void close() throws Exception {
		buckets.close();
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = processingTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}
}
