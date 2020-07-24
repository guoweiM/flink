import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.FileSinkSplit;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.splitsink.SplitWriter;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileSinkWriter<IN, BucketID> implements SplitWriter<IN, FileSinkSplit>, ProcessingTimeCallback {

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
		Sink.InitialContext context,
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval) throws Exception {

		final SinkInitialContext sinkInitialContext = (SinkInitialContext) context;

		this.buckets = bucketsBuilder.createBuckets(context.getSubtaskIndex());
		this.processingTimeService = sinkInitialContext.getProcessingTimeService();
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
