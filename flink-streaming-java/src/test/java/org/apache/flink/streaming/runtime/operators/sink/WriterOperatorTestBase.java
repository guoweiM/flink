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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.StreamRecordMatchers;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Base class for Tests for subclasses of {@link AbstractWriterOperator}.
 */
public abstract class WriterOperatorTestBase extends TestLogger {

	protected abstract AbstractWriterOperatorFactory<Integer, String> createWriterOperator(TestSink sink);

	@Test
	public void nonBufferingWriterEmitsWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.addWriter(new NonBufferingWriter())
						.setWriterStateSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addCommitter()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		testHarness.snapshot(1L, 1L);

		assertThat(
				testHarness.getOutput(), containStreamElements(
						new Watermark(initialTime),
						createStreamRecord(Tuple3.of(1, initialTime + 1, initialTime)),
						createStreamRecord(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	@Test
	public void nonBufferingWriterEmitsOnFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.addWriter(new NonBufferingWriter())
						.setWriterStateSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addCommitter()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				containStreamElements(
						new Watermark(initialTime),
						createStreamRecord(Tuple3.of(1, initialTime + 1, initialTime)),
						createStreamRecord(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	@Test
	public void bufferingWriterDoesNotEmitWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.addWriter(new BufferingWriter())
						.setWriterStateSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addCommitter()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		testHarness.snapshot(1L, 1L);

		assertThat(
				testHarness.getOutput(),
				containStreamElements(
						new Watermark(initialTime)));
	}

	@Test
	public void bufferingWriterEmitsOnFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.addWriter(new BufferingWriter())
						.setWriterStateSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addCommitter()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				containStreamElements(
						new Watermark(initialTime),
						createStreamRecord(Tuple3.of(1, initialTime + 1, initialTime)),
						createStreamRecord(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	@Test
	public void doNotSendCommittablesWhenThereIsNoCommitter() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.addWriter(new NonBufferingWriter())
						.setWriterStateSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		testHarness.snapshot(1L, 1L);

		assertThat(testHarness.getOutput().size(), equalTo(1));
		assertThat(
				testHarness.getOutput(), containStreamElements(
						new Watermark(initialTime)));
	}

	/**
	 * A {@link Writer} that returns all committables from {@link #prepareCommit(boolean)} without
	 * waiting for {@code flush} to be {@code true}.
	 */
	static class NonBufferingWriter extends TestSink.TestWriter {
		@Override
		public List<String> prepareCommit(boolean flush) {
			List<String> result = elements;
			elements = new ArrayList<>();
			return result;
		}

		@Override
		void restoredFrom(List<String> states) {

		}
	}

	/**
	 * A {@link Writer} that only returns committables from {@link #prepareCommit(boolean)} when
	 * {@code flush} is {@code true}.
	 */
	static class BufferingWriter extends TestSink.TestWriter {
		@Override
		public List<String> prepareCommit(boolean flush) {
			if (flush) {
				List<String> result = elements;
				elements = new ArrayList<>();
				return result;
			} else {
				return Collections.emptyList();
			}
		}

		@Override
		void restoredFrom(List<String> states) {

		}
	}

	//TODO:: move to util
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static org.hamcrest.Matcher<java.lang.Iterable<?>> containStreamElements(Object... items) {
		List<Matcher<?>> matchers = new ArrayList<>();
		for (Object item : items) {
			if (item instanceof Watermark) {
				matchers.add(IsEqual.equalTo(item));
			}
			if (item instanceof StreamRecord) {
				StreamRecord<byte[]> streamRecord = (StreamRecord<byte[]>) item;
				matchers.add(StreamRecordMatchers.streamRecord(
						streamRecord.getValue(),
						streamRecord.getTimestamp()));
			}
		}

		return new IsIterableContainingInOrder(matchers);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static org.hamcrest.Matcher<java.lang.Iterable<?>> containsStreamElementsInAnyOrder(Object... items) {
		List<Matcher<?>> matchers = new ArrayList<>();
		for (Object item : items) {
			if (item instanceof Watermark) {
				matchers.add(IsEqual.equalTo(item));
			}
			if (item instanceof StreamRecord) {
				StreamRecord<byte[]> streamRecord = (StreamRecord<byte[]>) item;
				matchers.add(StreamRecordMatchers.streamRecord(
						streamRecord.getValue(),
						streamRecord.getTimestamp()));
			}
		}

		return new IsIterableContainingInAnyOrder(matchers);
	}

	public StreamRecord<byte[]> createStreamRecord(Tuple3<Integer, Long, Long> tuple3) throws IOException {
		return new StreamRecord<>(TestSink.StringCommittableSerializer.INSTANCE.serialize(tuple3.toString()));
	}

	protected OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(
			TestSink sink) throws Exception {

		return new OneInputStreamOperatorTestHarness<>(
				createWriterOperator(sink),
				IntSerializer.INSTANCE);
	}
}
