/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.dag.Commit;
import org.apache.flink.streaming.api.dag.Map;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example for the new topology-based Sink API.
 */
public class SinkExample {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> input = env.fromElements(
				"guowei",
				"klou",
				"yun",
				"aljoscha");

		// see how nice it is to use the sink for a user
		input.sink(new MyStupidSink());

		env.execute();
	}

	/**
	 * Example sink that just prints commits when committing.
	 */
	public static class MyStupidSink extends Sink<String> {
		@Override
		public Transformation<?> apply(Context context, Transformation<String> input) {

			// instead of just forwarding we would have a transform/function that writes data and
			// forwards commits
			Transformation<String> mapped = input.apply(context, Map.of(new MySinkMapper()));

			// this is just an example, maybe we need more new primitive operations as building
			// blocks to allow building the StreamingFileSink and Kafka
			Transformation<Void> committed = mapped.apply(context, Commit.of(new MyCommitter()));

			return committed;
		}

		public static class MySinkMapper implements MapFunction<String, String> {
			@Override
			public String map(String value) {
				return "Hello " + value;
			}
		}

		public static class MyCommitter implements CommitFunction<String> {
			@Override
			public void commit(String commit) {
				System.out.println("Committing " + commit);
			}
		}
	}

}
