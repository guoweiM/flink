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
package org.apache.flink.test.state.operator.restore.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

public class Utils {

	public static DataStream<Integer> createSource(StreamExecutionEnvironment env, boolean restored) {
		DataStreamSource<Integer> source = env.addSource(new IntegerSource(restored));
		source.setParallelism(1);
		return source;
	}

	private static final class IntegerSource extends RichSourceFunction<Integer> {

		private static final long serialVersionUID = 1912878510707871659L;
		private final boolean restored;

		private volatile boolean running = true;

		public IntegerSource(boolean restored) {
			this.restored = restored;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ctx.collect(1);
			if (!restored) { // keep the job running until cancel-with-savepoint was done
				while (running) {
					Thread.sleep(500);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static SingleOutputStreamOperator<Integer> createPositiveMap(DataStream<Integer> input, boolean restored) {
		SingleOutputStreamOperator<Integer> positiveMap = input.map(new PositiveStatefulMap(restored));
		positiveMap.setParallelism(4);
		positiveMap.uid("positive");
		return positiveMap;
	}

	public static SingleOutputStreamOperator<Integer> createNegativeMap(DataStream<Integer> input, boolean restored) {
		SingleOutputStreamOperator<Integer> negativeMap = input.map(new NegativeStatefulMap(restored));
		negativeMap.setParallelism(4);
		negativeMap.uid("negative");
		return negativeMap;
	}


	private abstract static class CountingStatefulMap extends RichMapFunction<Integer, Integer> implements CheckpointedFunction {

		private static final long serialVersionUID = -7353315990238900737L;

		private final boolean restored;

		private transient ListState<Integer> state;
		private int restoredCount = 0;
		private final int valueToSet;

		CountingStatefulMap(boolean restored, int valueToSet) {
			this.restored = restored;
			this.valueToSet = valueToSet;
		}

		@Override
		public Integer map(Integer value) throws Exception {
			if (restored) {
				Preconditions.checkState(restoredCount == valueToSet, "State did not equal " + valueToSet + ".");
			}
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(valueToSet);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getSerializableListState("count");
			if (context.isRestored()) {
				Iterator<Integer> count = state.get().iterator();
				if (count.hasNext()) {
					this.restoredCount = count.next();
				} else {
					throw new IllegalStateException("Restored state was empty.");
				}
			}
		}
	}

	private static final class PositiveStatefulMap extends CountingStatefulMap {

		private static final long serialVersionUID = -7353315990238900737L;

		public PositiveStatefulMap(boolean restored) {
			super(restored, 1);
		}
	}

	private static final class NegativeStatefulMap extends CountingStatefulMap {

		private static final long serialVersionUID = -7353315990238900737L;

		public NegativeStatefulMap(boolean restored) {
			super(restored, -1);
		}
	}
}
