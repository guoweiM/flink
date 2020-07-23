package org.apache.flink.streaming.api.functions.sink.filesystem.poc3;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;

import java.io.IOException;

public class FileSinkCommitFunction<T> implements CommitFunction<FileSinkSplit> {

	private transient BucketWriter<T, ?> bucketWriter;

	private final Path basePath;

	private final Encoder<T> encoder;

    FileSinkCommitFunction(Path basePath, Encoder<T> encoder) {
        this.basePath = basePath;
        this.encoder = encoder;
    }

    @Override
    public void commit(FileSinkSplit split) {
        try {
            if (bucketWriter == null) {
				bucketWriter = new RowWiseBucketWriter(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
            }
            //TODO:: maybe need some clean up
			for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable : split.getPendingFileRecoverables()) {
				bucketWriter.recoverPendingFile(pendingFileRecoverable).commit();
			}
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("fail commit");
        }
    }
}
