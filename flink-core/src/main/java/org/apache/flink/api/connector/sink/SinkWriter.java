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

import java.io.IOException;

public interface SinkWriter<T> {

	void write(T t, Context context) throws Exception;

	void preCommit(long checkpointId) throws Exception;

	void commitUpTo(long checkpointId) throws IOException;

	void flush() throws IOException, Exception;

	/**
	 * TODO java doc.
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
