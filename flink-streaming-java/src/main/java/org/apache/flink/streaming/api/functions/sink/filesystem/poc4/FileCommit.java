package org.apache.flink.streaming.api.functions.sink.filesystem.poc4;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;

import java.io.IOException;

/**
 * TODO doc.
 * @param <T>
 */
public class FileCommit<T> implements CommitFunction<FileSplit> {

	private transient BucketWriter<T, ?> bucketWriter;

	private final Path basePath;

	private final Encoder<T> encoder;

	FileCommit(Path basePath, Encoder<T> encoder) {
		this.basePath = basePath;
		this.encoder = encoder;
	}

	@Override
	public void commit(FileSplit split) {
		try {
			if (bucketWriter == null) {
				bucketWriter = new RowWiseBucketWriter(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
			}

			if (split.getInProgressFileRecoverable() != null) {
				bucketWriter.cleanupInProgressFileRecoverable(split.getInProgressFileRecoverable());
			}
			if (split.getPendingFileRecoverable() != null) {
				bucketWriter.recoverPendingFile(split.getPendingFileRecoverable()).commit();
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("fail commit");
		}
	}
}
