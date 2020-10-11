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

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * The factory of {@link StatefulWriterOperator}.
 * @param <InputT>        The type of the sink's input
 * @param <CommT>         The type of information needed to commit data staged by the sink
 * @param <WriterStateT>  The type of the writer's state
 */
public final class StatefulWriterOperatorFactory<InputT, CommT, WriterStateT> extends AbstractWriterOperatorFactory<InputT, CommT> {

	private final Sink<InputT, CommT, WriterStateT, ?> sink;

	public StatefulWriterOperatorFactory(Sink<InputT, CommT, WriterStateT, ?> sink) {
		this.sink = sink;
	}

	@Override
	AbstractWriterOperator<InputT, CommT> createWriterOperator() {
		return new StatefulWriterOperator<>(sink, sink.getWriterStateSerializer().get());
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return StatefulWriterOperator.class;
	}
}
