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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UnifiedSinkOperatorFactory;

import java.util.Collection;
import java.util.List;

@Internal
public class UnifiedSinkTransformation<T> extends PhysicalTransformation<Object> {

	private final UnifiedSinkOperatorFactory<?, ?> sinkFactory;

	private final Transformation<T> input;

	public UnifiedSinkTransformation(
		Transformation<T> input,
		String name,
		UnifiedSinkOperatorFactory operatorFactory,
		int parallelism) {
		super(name, TypeExtractor.getForClass(Object.class), parallelism);
		this.sinkFactory = operatorFactory;
		this.input = input;
	}



	/**
	 * Returns the input {@code Transformation} of this {@code SinkTransformation}.
	 */
	public Transformation<T> getInput() {
		return input;
	}

	/**
	 * Returns the {@link StreamOperatorFactory} of this {@code SinkTransformation}.
	 */
	public StreamOperatorFactory<Object> getOperatorFactory() {
		return sinkFactory;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		sinkFactory.setChainingStrategy(strategy);
	}

}
