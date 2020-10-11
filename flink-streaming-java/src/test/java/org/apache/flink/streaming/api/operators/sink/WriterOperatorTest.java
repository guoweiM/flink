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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * Test the writer operator.
 */
public class WriterOperatorTest {

	@Test
	public void testStatelessWriter() throws Exception {

		final long initialTime = 0;
		final Supplier<StreamOperatorFactory<Tuple3<Integer, Long, Long>>> factorySupplier =
			() -> new StatelessWriterOperatorFactory<>(new StatelessWriterSink());

		final ConcurrentLinkedQueue<Object> expectedPreCommitOutput =
			new ConcurrentLinkedQueue<>(
				Arrays.asList(
					new Watermark(initialTime),
					new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime)),
					new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime))
				)
			);

		final ConcurrentLinkedQueue<Object> expectedEndOutput = new ConcurrentLinkedQueue<>(expectedPreCommitOutput);
		expectedEndOutput.add(new StreamRecord<>(DummyWriter.LAST_ELEMENT));

		final Consumer<OneInputStreamTaskTestHarness<Integer, Tuple3<Integer, Long, Long>>> process = task -> {
			task.processElement(new Watermark(initialTime));
			task.processElement(new StreamRecord<>(1, initialTime + 1));
			task.processElement(new StreamRecord<>(2, initialTime + 2));
		};

		final TaskStateSnapshot subtaskStates = processElements(
			null,
			factorySupplier,
			process,
			output -> Arrays.equals(expectedPreCommitOutput.toArray(), output),
			output -> Arrays.equals(expectedEndOutput.toArray(), output));

		// test after restoring
		processElements(
			subtaskStates,
			factorySupplier,
			process,
			output -> Arrays.equals(expectedPreCommitOutput.toArray(), output),
			output -> Arrays.equals(expectedEndOutput.toArray(), output));

	}

	@Test
	public void testStatefulWriter() throws Exception {

		final long initialTime = 0;
		final Supplier<StreamOperatorFactory<Tuple3<Integer, Long, Long>>> factorySupplier = () -> new StatefulWriterOperatorFactory<>(new StatefulWriterSink());

		final ConcurrentLinkedQueue<Object> expectedEndOutput1 =
			new ConcurrentLinkedQueue<>(
				Arrays.asList(
					new StreamRecord<>(Tuple3.of(1, initialTime + 1, Long.MIN_VALUE)),
					new StreamRecord<>(Tuple3.of(2, initialTime + 2, Long.MIN_VALUE)),
					new StreamRecord<>(DummyWriter.LAST_ELEMENT)
				)
			);

		final Consumer<OneInputStreamTaskTestHarness<Integer, Tuple3<Integer, Long, Long>>> process1 = task -> {
			task.processElement(new StreamRecord<>(1, initialTime + 1));
			task.processElement(new StreamRecord<>(2, initialTime + 2));
		};

		final TaskStateSnapshot subtaskStates = processElements(null,
			factorySupplier,
			process1,
			output -> Arrays.equals(new StreamRecord[0], output),
			output -> Arrays.equals(expectedEndOutput1.toArray(), output));

		final Consumer<OneInputStreamTaskTestHarness<Integer, Tuple3<Integer, Long, Long>>> process2 =
			task -> task.processElement(new StreamRecord<>(3, initialTime + 3));

		final ConcurrentLinkedQueue<Object> expectedPreCommitOutput2 = new ConcurrentLinkedQueue<>();
		expectedPreCommitOutput2.add(new StreamRecord<>(Tuple3.of(1, initialTime + 1, Long.MIN_VALUE)));
		expectedPreCommitOutput2.add(new StreamRecord<>(Tuple3.of(2, initialTime + 2, Long.MIN_VALUE)));
		expectedPreCommitOutput2.add(new StreamRecord<>(Tuple3.of(3, initialTime + 3, Long.MIN_VALUE)));

		final ConcurrentLinkedQueue<Object> expectedEndOutput2 = new ConcurrentLinkedQueue<>(expectedPreCommitOutput2);
		expectedEndOutput2.add(new StreamRecord<>(DummyWriter.LAST_ELEMENT));

		processElements(subtaskStates,
			factorySupplier,
			process2,
			output -> Arrays.equals(expectedPreCommitOutput2.toArray(), output),
			output -> Arrays.equals(expectedEndOutput2.toArray(), output));
	}

	private TaskStateSnapshot processElements(
		@Nullable TaskStateSnapshot taskStateSnapshot,
		Supplier<StreamOperatorFactory<Tuple3<Integer, Long, Long>>> factorySupplier,
		Consumer<OneInputStreamTaskTestHarness<Integer, Tuple3<Integer, Long, Long>>> process,
		Predicate<Object[]> verifyPreCommitOutput,
		Predicate<Object[]> verifyEndOutput) throws Exception {

		final long checkpointId = 1L;
		final long checkpointTimestamp = 1L;

		final OneInputStreamTaskTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness = new OneInputStreamTaskTestHarness<>(
			OneInputStreamTask::new,
			1, 1,
			BasicTypeInfo.INT_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Long.class, Long.class));

		if (taskStateSnapshot != null) {
			testHarness.setTaskStateSnapshot(checkpointId, taskStateSnapshot);
		}
		testHarness.setupOutputForSingletonOperatorChain();

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		final OperatorID operatorID = new OperatorID(38L, 3801L);
		streamConfig.setOperatorID(operatorID);
		streamConfig.setStreamOperatorFactory(factorySupplier.get());

		final TestTaskStateManager taskStateManagerMock = testHarness.getTaskStateManager();
		taskStateManagerMock.setWaitForReportLatch(new OneShotLatch());

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		final OneInputStreamTask<Integer, Tuple3<Integer, Long, Long>> task = testHarness.getTask();

		process.accept(testHarness);
		testHarness.waitForInputProcessing();

		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointTimestamp);

		// pre-commit
		task.triggerCheckpointAsync(checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation(), false);

		taskStateManagerMock.getWaitForReportLatch().await();

		assertEquals(checkpointId, taskStateManagerMock.getReportedCheckpointId());

		// remove CheckpointBarrier which is not expected
		testHarness.getOutput()
			.removeIf(record -> record instanceof CheckpointBarrier);

		Assert.assertTrue(verifyPreCommitOutput.test(testHarness.getOutput().toArray()));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		Assert.assertTrue(verifyEndOutput.test(testHarness.getOutput().toArray()));

		AbstractWriterOperator<Integer, Tuple3<Integer, Long, Long>> s =  testHarness.getHeadOperator();

		DummyWriter writer = (DummyWriter) s.getWriter();
		Assert.assertTrue(writer.isClosed());

		// set the operator state from previous attempt into the restored one
		return taskStateManagerMock.getLastJobManagerTaskStateSnapshot();
	}

	static final class StatelessWriterSink implements TestSink<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>, Void> {

		@Override
		public Writer<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>> createWriter(
			InitContext context, List<Tuple3<Integer, Long, Long>> states) {
			return new DummyWriter();
		}
	}

	static final class StatefulWriterSink implements TestSink<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>, Void> {

		@Override
		public Writer<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>> createWriter(InitContext context, List<Tuple3<Integer, Long, Long>> states) {
			return new DummyWriter(3, states);
		}

		@Override
		public Optional<SimpleVersionedSerializer<Tuple3<Integer, Long, Long>>> getWriterStateSerializer() {
			return Optional.of(new WriterStateSerializer());
		}
	}

	static final class DummyWriter
		implements Writer<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>> {

		static final Tuple3<Integer, Long, Long> LAST_ELEMENT = Tuple3.of(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);

		private boolean isClosed;

		private final int maxCacheElementNum;

		// element, timestamp, watermark
		private List<Tuple3<Integer, Long, Long>> elements;

		DummyWriter(int maxCacheElementNum, List<Tuple3<Integer, Long, Long>> restoreElements) {
			this.isClosed = false;
			this.elements = new ArrayList<>(restoreElements);
			this.maxCacheElementNum = maxCacheElementNum;
		}

		DummyWriter() {
			this(0, Collections.emptyList());
		}

		@Override
		public void write(Integer element, Context context) {
			elements.add(Tuple3.of(element, context.timestamp(), context.currentWatermark()));
		}

		@Override
		public List<Tuple3<Integer, Long, Long>> prepareCommit(boolean flush) {
			final List<Tuple3<Integer, Long, Long>> r = elements;
			if (flush) {
				elements.add(LAST_ELEMENT);
				return elements;
			} else if (elements.size() >= maxCacheElementNum) {
				elements = new ArrayList<>();
				return r;
			} else {
				return Collections.emptyList();
			}
		}

		@Override
		public List<Tuple3<Integer, Long, Long>> snapshotState() {
			return elements;
		}

		@Override
		public void close() {
			isClosed = true;
		}

		public boolean isClosed() {
			return isClosed;
		}
	}

	static final class WriterStateSerializer implements SimpleVersionedSerializer<Tuple3<Integer, Long, Long>> {

		@Override
		public int getVersion() {
			return 0;
		}

		@Override
		public byte[] serialize(Tuple3<Integer, Long, Long> tuple3) throws IOException {
			return InstantiationUtil.serializeObject(tuple3);

		}

		@Override
		public Tuple3<Integer, Long, Long>  deserialize(int version, byte[] serialized) throws IOException {
			try {
				return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Failed to deserialize the writer's state.", e);
			}
		}
	}

	interface TestSink<InputT, CommT, WriterStateT, GlobalCommT> extends Sink<InputT, CommT, WriterStateT, GlobalCommT> {

		@Override
		default Optional<Committer<CommT>> createCommitter() {
			return Optional.empty();
		}

		@Override
		default Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() {
			return Optional.empty();
		}

		@Override
		default Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
			return Optional.empty();
		}

		@Override
		default Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
			return Optional.empty();
		}

		@Override
		default Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
			return Optional.empty();
		}
	}
}
