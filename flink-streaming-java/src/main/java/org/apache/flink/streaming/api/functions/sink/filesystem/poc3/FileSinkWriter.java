

package org.apache.flink.streaming.api.functions.sink.filesystem.poc3;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileSinkWriter<IN, BucketID> implements ProcessingTimeCallback {

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
		new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
		new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	final Buckets<IN, BucketID> buckets;

	private final ListState<byte[]> bucketStatesContainer;

	private final ListState<Long> maxPartCountersState;

	private final ProcessingTimeService processingTimeService;

	private final long bucketCheckInterval;

	public FileSinkWriter(
		int subtaskIndex,
		Boolean isRestored,
		OperatorStateStore operatorStateStore,
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval) throws Exception {


		this.buckets = bucketsBuilder.createBuckets(subtaskIndex);
		this.processingTimeService = null;
		this.bucketCheckInterval = bucketCheckInterval;

		this.bucketStatesContainer = operatorStateStore.getListState(BUCKET_STATE_DESC);
		this.maxPartCountersState = operatorStateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);
		if (isRestored) {
			this.buckets.initializeState(bucketStatesContainer, maxPartCountersState);
		}

	}

	public void write(IN element) throws Exception {
		buckets.onElement(element, 0, null, 0);
	}

	public List<FileSinkSplit> preCommit() throws IOException {
		return buckets.preCommit();
	}

	public void persist() throws Exception {
		buckets.persist(bucketStatesContainer, maxPartCountersState);
	}

	public void flush() {
		buckets.flush();
	}

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
