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

import org.apache.flink.api.connector.sink.SplitCommitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO DOC.
 */
public class FileSinkSplitCommitter<IN, BucketID> implements SplitCommitter<FileSinkSplit> {

	final Map<Integer, List<FileSinkSplit>> sinkSplitsPerTask;
	final BucketWriter<IN, BucketID> bucketWriter;

	public FileSinkSplitCommitter(BucketWriter<IN, BucketID> bucketWriter) {
		this.bucketWriter = bucketWriter;
		this.sinkSplitsPerTask = new HashMap<>();
	}

	@Override
	public void commit(List<FileSinkSplit> checkpoints) throws IOException {
		for (FileSinkSplit fileSinkSplit : checkpoints) {
			if (fileSinkSplit.getInProgressFileRecoverable() != null) {
				bucketWriter.cleanupInProgressFileRecoverable(fileSinkSplit.getInProgressFileRecoverable());
			}

			for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable : fileSinkSplit.getPendingFileRecoverables()) {
				bucketWriter.recoverPendingFile(pendingFileRecoverable).commit();
			}
		}
	}

	@Override
	public void close() throws Exception {

	}
}
