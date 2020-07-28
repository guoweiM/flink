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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.dag.CommitTransformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.sink.Sink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

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
		public Transformation<?> apply(Transformation<String> input) {

			// the Transformation API is not very ergonomic right now, we could think of adding
			// a minimal Transformation.apply() method that would allow stringing Transformations
			// together but I hope you get the idea.

			StreamMap<String, String> mapOperator = new StreamMap<>(new MySinkMapper());
			OneInputTransformation<String, String> mapped = new OneInputTransformation<>(
					input,
					"mapper",
					mapOperator,
					input.getOutputType(),
					input.getParallelism());

			// instead of just forwarding we would have a transform/function that writes data and
			// forwards commits

			// this is just an example, maybe we need more new primitive operations as building
			// blocks to allow building the StreamingFileSink and Kafka
			CommitTransformation<String> commitTransformation = new CommitTransformation<>(
					mapped,
					new MyCommitter(),
					StringSerializer.INSTANCE);

			return commitTransformation;
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
