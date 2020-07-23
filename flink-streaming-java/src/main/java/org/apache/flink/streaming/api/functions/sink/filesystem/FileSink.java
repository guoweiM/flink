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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink.SinkWriterContext;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.splitsink.SplitCommitter;
import org.apache.flink.streaming.api.functions.splitsink.SplitSink;
import org.apache.flink.streaming.api.functions.splitsink.SplitWriter;

import java.io.IOException;


/**
 * TODO DOC.
 * @param <IN>
 * @param <BucketID>
 */
public class FileSink<IN, BucketID> extends SplitSink<IN, FileSinkSplit> {

	private static final long serialVersionUID = -4030601241044738462L;


	private final StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	private final long bucketCheckInterval;

	private final Path basePath;

	private final Encoder<IN> encoder;

	public FileSink(
		StreamingFileSink.BucketsBuilder<IN, BucketID, ? extends StreamingFileSink.BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
		long bucketCheckInterval,
		Path path,
		Encoder<IN> encoder) {
		this.bucketsBuilder = bucketsBuilder;
		this.bucketCheckInterval = bucketCheckInterval;
		this.basePath = path;
		this.encoder = encoder;
	}

	@Override
	public SplitWriter<IN, FileSinkSplit> createSplitWriter(SinkWriterContext sinkWriterContext) throws Exception {
		return  new FileSinkWriter(sinkWriterContext, bucketsBuilder, bucketCheckInterval);
	}

	public SplitCommitter<FileSinkSplit> createSplitCommitter() {
		try {
			return new FileSinkSplitCommitter(new RowWiseBucketWriter(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("fail to create committer");
		}
	}

	@Override
	public SimpleVersionedSerializer<FileSinkSplit> getSplitSerializer() {
		return null;
	}

}
