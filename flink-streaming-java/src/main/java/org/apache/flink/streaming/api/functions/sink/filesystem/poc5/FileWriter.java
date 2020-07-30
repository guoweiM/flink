package org.apache.flink.streaming.api.functions.sink.filesystem.poc5;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.streaming.api.dag.Commit;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileWriter<IN, BucketID> implements Writer<IN, CommittableFiles>, ProcessingTimeCallback {

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
		new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
		new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	private final ProcessingTimeService processingTimeService;

	private final long bucketCheckInterval;

	private final StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, ?, ?>> bucketsBuilder;

	private final Buckets<IN, BucketID> buckets;

	private final ListState<byte[]> bucketStatesContainer;

	private final ListState<Long> maxPartCountersState;

	public FileWriter(
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval,
		USink.InitialContext context) throws Exception {

		this.processingTimeService = null;
		this.bucketCheckInterval = bucketCheckInterval;

		this.buckets = bucketsBuilder.createBuckets(context.getSubtaskIndex());
		this.bucketsBuilder = bucketsBuilder;

		this.bucketStatesContainer = context.getListState(BUCKET_STATE_DESC);
		this.maxPartCountersState = context.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

		if (context.isRestored()) {
			this.buckets.initializeState(bucketStatesContainer, maxPartCountersState);
		}

	}

	@Override
	public void write(IN in, Context context, Collector<CommittableFiles> output) throws Exception {
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
	public void persistent(Collector<CommittableFiles> output) throws Exception {
		buckets.persist(output, bucketStatesContainer, maxPartCountersState);
	}

	@Override
	public void flush(Collector<CommittableFiles> output) throws IOException {
		buckets.flush(output);
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
