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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.GlobalStreamingCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatefulWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatelessWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterOperatorFactory;

import java.util.Collection;
import java.util.Collections;

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
		return null;
	}

	@Override
	public Collection<Integer> translateForStreaming(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		//TODO:: we could get whether the writer and committer has the downstream from the two boolean

		final boolean hasCommitter = transformation
				.getSink()
				.getCommittableSerializer()
				.isPresent();
		final boolean hasGlobalCommitter = transformation
				.getSink()
				.getGlobalCommittableSerializer()
				.isPresent();

		final OneInputTransformation<InputT, CommT> writer =
				createAndTranslateStreamingWriter(transformation, context);

		final OneInputTransformation<CommT, CommT> committer;
		if (hasCommitter) {
			committer = createAndTranslateStreamingCommitter(writer, transformation, context);
		} else {
			committer = null;
		}
		if (hasGlobalCommitter) {
			createAndTranslateStreamingGlobalCommitter(
					hasCommitter ? committer : writer,
					transformation,
					context);
		}
		return Collections.emptyList();
	}

	private OneInputTransformation<InputT, CommT> createAndTranslateStreamingWriter(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final String name = "Sink Writer: " + transformation.getName();
		final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = transformation.getSink();
		final TypeInformation<CommT> committableTypeInfo = TypeExtractor.createTypeInfo(
				Sink.class,
				sink.getClass(),
				1,
				null,
				null);
		@SuppressWarnings("unchecked")
		final Transformation<InputT> input = (Transformation<InputT>) transformation
				.getInputs()
				.get(0);
		final int parallelism = getParallelism(transformation, context);

		final OneInputTransformation<InputT, CommT> writer = sink
				.getWriterStateSerializer()
				.map(s -> new OneInputTransformation<>(
						input,
						name,
						new StatefulWriterOperatorFactory<>(sink),
						committableTypeInfo,
						parallelism))
				.orElseGet(() -> new OneInputTransformation<>(
						input,
						name,
						new StatelessWriterOperatorFactory<>(sink),
						committableTypeInfo,
						parallelism));
		inheritPropertiesFromSinkTransformation(writer, transformation);
		final OneInputTransformationTranslator<InputT, CommT> writerTranslator = new OneInputTransformationTranslator<>();
		writerTranslator.translateForStreaming(writer, context);
		return writer;
	}

	private OneInputTransformation<CommT, CommT> createAndTranslateStreamingCommitter(
			Transformation<CommT> input,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = transformation.getSink();
		final TypeInformation<CommT> committableTypeInfo = TypeExtractor.createTypeInfo(
				Sink.class,
				sink.getClass(),
				1,
				null,
				null);
		final int parallelism = getParallelism(transformation, context);

		final OneInputTransformation<CommT, CommT> committer = new OneInputTransformation<>(
				input,
				"Sink Committer: " + transformation.getName(),
				new StreamingCommitterOperatorFactory<>(sink),
				committableTypeInfo,
				parallelism);
		inheritPropertiesFromSinkTransformation(committer, transformation);
		final OneInputTransformationTranslator<CommT, CommT> committerTranslator = new OneInputTransformationTranslator<>();
		committerTranslator.translateForStreaming(committer, context);
		return committer;
	}

	private void createAndTranslateStreamingGlobalCommitter(
			Transformation<CommT> input,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = transformation.getSink();

		final OneInputTransformation<CommT, GlobalCommT> globalCommitter = new OneInputTransformation<>(
				input,
				"Sink Global Committer: " + transformation.getName(),
				new GlobalStreamingCommitterOperatorFactory<>(sink),
				null,
				1);
		final OneInputTransformationTranslator<CommT, GlobalCommT> globalCommitterTranslator =
				new OneInputTransformationTranslator<>();
		inheritPropertiesFromSinkTransformation(globalCommitter, transformation);
		globalCommitterTranslator.translateForStreaming(globalCommitter, context);
	}

	private void inheritPropertiesFromSinkTransformation(
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
	}

	private int getParallelism(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			Context context) {
		return sinkTransformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? sinkTransformation.getParallelism()
				: context.getStreamGraph().getExecutionConfig().getParallelism();
	}
}
