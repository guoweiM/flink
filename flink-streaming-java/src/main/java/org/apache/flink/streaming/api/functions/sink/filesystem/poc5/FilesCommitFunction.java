package org.apache.flink.streaming.api.functions.sink.filesystem.poc5;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;

import java.io.IOException;

/**
 * TODO doc.
 * @param <T>
 */
public class FilesCommitFunction<T> implements CommitFunction<CommittableFiles> {

	private transient BucketWriter<T, ?> bucketWriter;

	private final Path basePath;

	private final Encoder<T> encoder;

	FilesCommitFunction(Path basePath, Encoder<T> encoder) {
		this.basePath = basePath;
		this.encoder = encoder;
	}

	@Override
	public void commit(CommittableFiles split) {
		try {
			if (bucketWriter == null) {
				bucketWriter = new RowWiseBucketWriter(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
			}

			if (split.getInProgressFileRecoverable() != null) {
				bucketWriter.cleanupInProgressFileRecoverable(split.getInProgressFileRecoverable());
			}

			for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable : split.getPendingFileRecoverables()) {
				bucketWriter.recoverPendingFile(pendingFileRecoverable).commit();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("fail commit");
		}
	}
}
