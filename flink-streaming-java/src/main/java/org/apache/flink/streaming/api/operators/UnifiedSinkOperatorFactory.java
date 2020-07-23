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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.sink.coordinator.SinkCoordinatorProvider;

public class UnifiedSinkOperatorFactory<IN, CheckpointT> extends AbstractStreamOperatorFactory<Object>
	implements CoordinatedOperatorFactory<Object>{

	private final Sink<IN, CheckpointT> sink;

	public UnifiedSinkOperatorFactory(Sink<IN, CheckpointT> sink) {
		this.sink = sink;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new SinkCoordinatorProvider<>(operatorID, sink);
	}

	@Override
	public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();

		try {
			UnifiedSinkOperator u = new UnifiedSinkOperator<>(
				sink,
				parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId),
				parameters.getProcessingTimeService(),
				null);

			u.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
			return (T) u;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("fail to create stream operator");
		}
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return UnifiedSinkOperator.class;
	}
}
