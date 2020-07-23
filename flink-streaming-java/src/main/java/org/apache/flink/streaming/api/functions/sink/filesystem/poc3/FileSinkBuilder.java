package org.apache.flink.streaming.api.functions.sink.filesystem.poc3;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.dag.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.dag.Commit;
import org.apache.flink.streaming.api.dag.Map;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class FileSinkBuilder<T> extends Sink<T> {

	private final StreamingFileSink.BucketsBuilder<T, ?, ? extends StreamingFileSink.BucketsBuilder<T, ?, ?>> bucketsBuilder;

	private final Path basePath;

	private final Encoder<T> encoder;

	public FileSinkBuilder(
		StreamingFileSink.BucketsBuilder<T, ?, ? extends StreamingFileSink.BucketsBuilder<T, ?, ?>> bucketsBuilder, Path basePath, Encoder<T> encoder) {

		this.bucketsBuilder = bucketsBuilder;
		this.basePath = basePath;
		this.encoder = encoder;
	}

	@Override
	public Transformation<?> apply(Context context, Transformation<T> input) {
		/**
		 * Produce the {@link FileSinkSplit}. The split represents the data that could be committed to the file system.
		 */
		final Transformation<FileSinkSplit> mapped = input.apply(context, Map.of(new FileSinkFunction<>(bucketsBuilder)));

		/**
		 *
		 * 1. The {@link org.apache.flink.api.dag.CommitTransformation} means that FLINK would call the
		 *    {@link org.apache.flink.api.common.functions.CommitFunction} with a commit only when
		 *    2.1 The commit is materialized. It means the data "in" the current commit would not be in any other
		 *        following different commit. (The data that would be committed to the external system should belongs to
		 *        one commit and only belong to one commit.)
		 *    2.2 The dependent commits has already finished.(still an open question.)
		 * 2. The {@link org.apache.flink.api.common.functions.CommitFunction} should be idempotent.
		 * 3. There might be many ways to materialized the input of the commit transformation. It depends on the the execution mode and ???
		 * 4. Open question: What is relation between the multiple commit transformations.
		 */
		final Transformation<Void> commit = (Transformation<Void>) mapped.apply(context, Commit.of(new FileSinkCommitFunction(basePath, encoder)));
		return commit;
	}

}
