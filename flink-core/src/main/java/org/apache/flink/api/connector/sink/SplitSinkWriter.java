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

package org.apache.flink.api.connector.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * The writer is responsible for managing the stage of data that has not been committed to the external system. The data
 * generally goes through two stages before being committed to the external system. The first stage is the preparation.
 * Thanks to some conditions that have not been met the element in the preparation stage could not be committed to the
 * external system. The second stage is committable. The element is this stage could be committed to the external system.
 *
 * @param <T> 		 The type of writer's input
 * @param <SplitT>   The type of data that is ready to be committed to the external system.
 */
public interface SplitSinkWriter<T, SplitT> {

	//TODO:: Is that a good desgin pattern for the ?
	void open(InitialContext<SplitT> context) throws Exception;

	//TODO:: maybe this operator could do some optimization if it knows batch or blocking mode????
	/**
	 * Add a element to the writer.
	 * @param t 		The input record.
	 * @param context   The additional information about the input record.
	 */
	void write(T t, Context context) throws Exception;

	/**
	 * Persistent the state of the writer.
	 * TODO:: Maybe we should not expose the internal checkpoint's mechanism such as checkpoint id things to the user API.
	 */
	void persistent() throws Exception;

	/**
	 * Flush all the data in the preparation stage to the committable stage.
	 */
	void flush() throws IOException;

	/**
	 * Release all the resources.
	 */
	void close();

	/**
	 * TODO:: Java Doc.
	 */
	interface Context {

		/** Returns the current processing time. */
		long currentProcessingTime();

		/** Returns the current event-time watermark. */
		long currentWatermark();

		/**
		 * Returns the timestamp of the current input record or {@code null} if the element does not
		 * have an assigned timestamp.
		 */
		Long timestamp();
	}

	/**
	 * Java Doc.
	 */
	interface InitialContext<SplitT> {

		boolean isRestored();

		int getSubtaskIndex();

		Collector<SplitT> getCollector();

		//TODO :: key or non-key does the sink developer knows? TBV
		<S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

		<S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception;
	}
}
