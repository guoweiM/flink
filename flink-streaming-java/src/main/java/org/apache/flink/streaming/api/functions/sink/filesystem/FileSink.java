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

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink.SplitCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;


/**
 * TODO DOC.
 * @param <IN>
 * @param <BucketID>
 */
public class FileSink<IN, BucketID> implements Sink<IN, FileSinkSplit, FileSinkSplit> {

	private final StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	private final BucketWriter<IN, BucketID> bucketWriter;

	private final long bucketCheckInterval;

	public FileSink(
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		BucketWriter<IN, BucketID> bucketWriter,
		long bucketCheckInterval) {
		this.bucketsBuilder = bucketsBuilder;
		this.bucketWriter = bucketWriter;
		this.bucketCheckInterval = bucketCheckInterval;
	}

	@Override
	public SinkWriter<IN, FileSinkSplit> createWriter(InitialContext context) throws Exception {
		return new FileWriter<>(context, bucketsBuilder, bucketCheckInterval);
	}

	public SplitCommitter<FileSinkSplit> createSplitCommitter() {
		return new FileSinkSplitCommitter(bucketWriter);
	}

	@Override
	public SimpleVersionedSerializer<FileSinkSplit> getSplitSerializer() {
		return null;
	}

	/**
	 * TODO: where to put this context
	 */
	interface FileSinkInitialContext extends InitialContext {

		ProcessingTimeService getProcessingTimeService();
	}
}
