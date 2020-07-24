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

package org.apache.flink.streaming.api.functions.splitsink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkManager;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink.SinkWriterContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;

public abstract class SplitSink<T, SplitT> implements Sink<T, List<SplitT>> {

	public abstract SplitWriter<T, SplitT> createSplitWriter();

	public abstract SplitCommitter<SplitT> createSplitCommitter();

	public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

	public SinkWriter<T> createWriter(SinkWriterContext sinkWriterContext) throws Exception {
		return new SplitSinkWriter<>(
			sinkWriterContext.isRestored(),
			createSplitWriter(),
			createSplitCommitter(),
			getSplitSerializer(),
			sinkWriterContext);
	}

	public SinkManager<List<SplitT>> createSinkManager() {
		return new SplitSinkManager<>(createSplitCommitter());
	}
}
