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

import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * The writer is responsible for managing the stage of data that has not been committed to the external system. The data
 * generally goes through two stages before being committed to the external system. The first stage is the preparation.
 * Thanks to some conditions that have not been met the element in the preparation stage could not be committed to the
 * external system. The second stage is committable. The element is this stage could be committed to the external system.
 *
 * @param <T> 		 		The type of writer's input
 * @param <CommittableT>   	The type of handle that is ready to be committed to the external system.
 */
public interface Writer<T, CommittableT> extends AutoCloseable {

	/**
	 * Add a element to the writer.
	 * @param t 		The input record.
	 * @param context   The additional information about the input record.
	 */
	void write(T t, Context context, Collector<CommittableT> collector) throws Exception;

	/**
	 * Persistent the state of the writer and return the some
	 * TODO:: if we have good state interface maybe we should not expose this interface at all
	 */
	void persistent(Collector<CommittableT> output) throws Exception;

	/**
	 * Flush all the data in the preparation stage to the committable stage.
	 */
	void flush(Collector<CommittableT> output) throws IOException;

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
}
