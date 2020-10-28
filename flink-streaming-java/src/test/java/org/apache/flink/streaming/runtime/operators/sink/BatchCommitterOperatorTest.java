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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.SinkTestUtil.containsStreamElementsInAnyOrder;
import static org.apache.flink.streaming.util.SinkTestUtil.convertStringListToByteArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link BatchCommitterOperator}.
 */
public class BatchCommitterOperatorTest extends TestLogger {

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutCommitter() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(null);
		testHarness.initializeEmptyState();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void doNotSupportRetry() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(new TestSink.AlwaysRetryCommitter());

		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.processElement(new StreamRecord<>(TestSink.StringCommittableSerializer.INSTANCE.serialize(
				"those")));
		testHarness.endInput();
		testHarness.close();
	}

	@Test
	public void commit() throws Exception {

		final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				committer);

		final List<String> stringInput = Arrays.asList("youth", "laugh", "nothing");
		final List<byte[]> byteInput = convertStringListToByteArrayList(stringInput);
		final List<String> expectedStringOutput = stringInput;
		final List<byte[]> expectedByteOutput = convertStringListToByteArrayList(
				expectedStringOutput);

		testHarness.initializeEmptyState();
		testHarness.open();

		testHarness.processElements(byteInput.stream().map(StreamRecord::new).collect(
				Collectors.toList()));
		testHarness.endInput();
		testHarness.close();

		assertThat(
				committer.getCommittedData(),
				containsInAnyOrder(expectedStringOutput.toArray()));

		assertThat(testHarness.getOutput(), containsStreamElementsInAnyOrder(expectedByteOutput
				.stream()
				.map(StreamRecord::new)
				.toArray()));
	}

	@Test
	public void close() throws Exception {
		final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				committer);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.close();

		assertThat(committer.isClosed(), is(true));
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(Committer<String> committer) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
				new BatchCommitterOperatorFactory<>(TestSink
						.newBuilder()
						.addWriter()
						.addCommitter(committer)
						.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addGlobalCommitter()
						.build()),
				BytePrimitiveArraySerializer.INSTANCE);
	}
}
