package org.apache.flink.streaming.api.functions.sink.filesystem.poc5;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * TODO java doc.
 * @param <IN>
 * @param <BucketID>
 */
public class FileSink<IN, BucketID> implements USink<IN, CommittableFiles> {

	final StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	final long bucketCheckInterval;

	final Path basePath;

	final Encoder<IN> encoder;

	public FileSink(
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval,
		Path basePath,
		Encoder<IN> encoder) {
		this.bucketsBuilder = bucketsBuilder;
		this.bucketCheckInterval = bucketCheckInterval;
		this.basePath = basePath;
		this.encoder = encoder;
	}

	@Override
	public Writer<IN, CommittableFiles> createWriter(InitialContext context) throws Exception {
		return new FileWriter(bucketsBuilder, bucketCheckInterval, context);
	}

	@Override
	public CommitFunction<CommittableFiles> createCommitFunction() {
		return new FilesCommitFunction(basePath, encoder);
	}
}
