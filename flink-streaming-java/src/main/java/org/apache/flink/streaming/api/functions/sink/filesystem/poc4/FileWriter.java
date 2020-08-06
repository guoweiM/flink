package org.apache.flink.streaming.api.functions.sink.filesystem.poc4;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.SplitSinkWriter;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileWriter<IN, BucketID> implements SplitSinkWriter<IN, FileSplit>, ProcessingTimeCallback {

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
		new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
		new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	private final ProcessingTimeService processingTimeService;

	private final long bucketCheckInterval;

	private final StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, ?, ?>> bucketsBuilder;

	// ============================= Runtime Fields ===========================================

	private Buckets<IN, BucketID> buckets;

	private ListState<byte[]> bucketStatesContainer;

	private ListState<Long> maxPartCountersState;

	public FileWriter(
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval) {

		this.processingTimeService = null;
		this.bucketCheckInterval = bucketCheckInterval;

		this.bucketsBuilder = bucketsBuilder;

	}

	@Override
	public void open(InitialContext<FileSplit> context) throws Exception {
		buckets = bucketsBuilder.createBuckets(context.getSubtaskIndex(), context.getCollector());

		bucketStatesContainer = context.getListState(BUCKET_STATE_DESC);
		maxPartCountersState = context.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

		if (context.isRestored()) {
			this.buckets.initializeState(bucketStatesContainer, maxPartCountersState);
		}

	}

	@Override
	public void write(IN in, Context context) throws Exception {
		buckets.onElement(in, new SinkFunction.Context() {
			@Override
			public long currentProcessingTime() {
				return 0;
			}

			@Override
			public long currentWatermark() {
				return 0;
			}

			@Override
			public Long timestamp() {
				return null;
			}
		});
	}

	@Override
	public void persistent() throws Exception {
		//TODO:: need other way to clean up the non-active buckets
		buckets.snapshotState(-10000, bucketStatesContainer, maxPartCountersState);
	}

	@Override
	public void flush() throws IOException {
		buckets.flush();
	}

	public void close() {
		buckets.close();
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = processingTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}
}
