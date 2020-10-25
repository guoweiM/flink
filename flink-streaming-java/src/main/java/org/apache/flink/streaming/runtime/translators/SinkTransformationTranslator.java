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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.BatchCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.BatchGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.GlobalStreamingCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatefulWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatelessWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterOperatorFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * TODO java doc.
 *
 * @param <InputT>
 * @param <CommT>
 * @param <WriterStateT>
 * @param <GlobalCommT>
 */
public class SinkTransformationTranslator<InputT, CommT, WriterStateT, GlobalCommT> implements
		TransformationTranslator<Object, SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT>> {

	@Override
	public Collection<Integer> translateForBatch(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = transformation.getSink();
		final OneInputTransformation<InputT, CommT> writer = translateWriter(
				transformation,
				context);
		final Optional<OneInputTransformation<CommT, CommT>> committer = translateCommitter(
				writer,
				transformation,
				new BatchCommitterOperatorFactory<>(sink),
				context);

		translateGlobalCommitter(
				committer.isPresent() ? committer.get() : writer,
				transformation,
				new BatchGlobalCommitterOperatorFactory<>(sink),
				context);
		return Collections.emptyList();
	}

	@Override
	public Collection<Integer> translateForStreaming(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = transformation.getSink();
		final OneInputTransformation<InputT, CommT> writer = translateWriter(
				transformation,
				context);
		final Optional<OneInputTransformation<CommT, CommT>> committer = translateCommitter(
				writer,
				transformation,
				new StreamingCommitterOperatorFactory<>(sink),
				context);

		translateGlobalCommitter(
				committer.isPresent() ? committer.get() : writer,
				transformation,
				new GlobalStreamingCommitterOperatorFactory<>(sink),
				context);

		return Collections.emptyList();
	}

	private OneInputTransformation<InputT, CommT> translateWriter(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final String name = "Sink Writer: " + transformation.getName();
		final TypeInformation<CommT> committableTypeInfo = TypeExtractor.createTypeInfo(
				Sink.class,
				transformation.getSink().getClass(),
				1,
				null,
				null);
		@SuppressWarnings("unchecked")
		final Transformation<InputT> input = (Transformation<InputT>) transformation
				.getInputs()
				.get(0);
		final int parallelism = getParallelism(transformation, context);

		final OneInputTransformation<InputT, CommT> writer = transformation
				.getSink()
				.getWriterStateSerializer()
				.map(s -> new OneInputTransformation<>(
						input,
						name,
						new StatefulWriterOperatorFactory<>(transformation.getSink()),
						committableTypeInfo,
						parallelism))
				.orElseGet(() -> new OneInputTransformation<>(
						input,
						name,
						new StatelessWriterOperatorFactory<>(transformation.getSink()),
						committableTypeInfo,
						parallelism));
		inheritPropertiesFromSinkTransformation("Writer", writer, transformation);
		context.translate(writer);
		return writer;
	}

	private Optional<OneInputTransformation<CommT, CommT>> translateCommitter(
			Transformation<CommT> input,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			OneInputStreamOperatorFactory<CommT, CommT> operatorFactory,
			Context context) {

		if (!transformation.getSink().getCommittableSerializer().isPresent()) {
			return Optional.empty();
		}

		final TypeInformation<CommT> committableTypeInfo = TypeExtractor.createTypeInfo(
				Sink.class,
				transformation.getSink().getClass(),
				1,
				null,
				null);

		final int parallelism = getParallelism(transformation, context);

		final OneInputTransformation<CommT, CommT> committer = new OneInputTransformation<>(
				input,
				"Sink Committer: " + transformation.getName(),
				operatorFactory,
				committableTypeInfo,
				parallelism);
		inheritPropertiesFromSinkTransformation("Committer", committer, transformation);
		context.translate(committer);
		return Optional.of(committer);
	}

	private void translateGlobalCommitter(
			Transformation<CommT> input,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			OneInputStreamOperatorFactory<CommT, GlobalCommT> operatorFactory,
			Context context) {

		if (!transformation.getSink().getGlobalCommittableSerializer().isPresent()) {
			return;
		}
		final OneInputTransformation<CommT, GlobalCommT> globalCommitter = new OneInputTransformation<>(
				input,
				"Sink Global Committer: " + transformation.getName(),
				operatorFactory,
				null,
				1);
		inheritPropertiesFromSinkTransformation(
				"Global Committer",
				globalCommitter,
				transformation);
		context.translate(globalCommitter);
	}

	private void inheritPropertiesFromSinkTransformation(
			String uidPrefix,
			OneInputTransformation<?, ?> transformation,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation) {

		// currently we only inherit the properties that could be set from DataStreamSink

		final String slotSharingGroup = sinkTransformation.getSlotSharingGroup();
		if (slotSharingGroup != null) {
			transformation.setSlotSharingGroup(slotSharingGroup);
		}

		final ChainingStrategy chainingStrategy = sinkTransformation.getChainingStrategy();
		if (chainingStrategy != null) {
			transformation.setChainingStrategy(chainingStrategy);
		}

		final String uid = sinkTransformation.getUid();
		if (uid != null) {
			transformation.setUid(String.format("Sink %s %s", uidPrefix, uid));
		}

	}

	private int getParallelism(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			Context context) {
		return sinkTransformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? sinkTransformation.getParallelism()
				: context.getStreamGraph().getExecutionConfig().getParallelism();
	}
}
