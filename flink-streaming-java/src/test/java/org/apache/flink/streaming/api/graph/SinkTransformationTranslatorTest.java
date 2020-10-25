/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.streaming.runtime.operators.sink.WriterOperatorTestBase;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class SinkTransformationTranslatorTest extends TestLogger {

	@Test
	public void generateWriterTopology() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// This will throw exception because that lambda class(line TestSink::57)
		// would has some reference which does not serializable
		DataStreamSink<Integer> dataStreamSink =
				env.fromElements(1, 2).addSink(
						TestSink.newBuilder()
								.addWriter().addCommitter().addGlobalCommitter().build());

		StreamGraph streamGraph = env.getStreamGraph("test");
		System.err.println(streamGraph.toString());
	}
}
