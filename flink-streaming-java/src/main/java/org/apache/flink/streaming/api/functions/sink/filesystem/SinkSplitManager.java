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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.connector.sink.SplitCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * TODO java Doc.
 * @param <SplitT>
 */
class SinkSplitManager<SplitT> implements AutoCloseable {

	private final SplitCommitter<SplitT> splitCommitter;

	private final NavigableMap<Long, Map<Integer, List<SplitT>>> subTaskSplitsPerCheckpoint;

	private Map<Integer, List<SplitT>> currentSubTaskSplits;

	SinkSplitManager(SplitCommitter<SplitT> splitCommitter) {
		this.splitCommitter = splitCommitter;
		this.subTaskSplitsPerCheckpoint = new TreeMap<>();
		this.currentSubTaskSplits = new HashMap<>();
	}

	void add(int subTaskId, List<SplitT> splits) {
		currentSubTaskSplits.put(subTaskId, splits);
	}

	void remove(int subTaskId) {
		currentSubTaskSplits.remove(subTaskId);
	}

	List<SplitT> persist(long checkpointId) {
		subTaskSplitsPerCheckpoint.put(checkpointId, currentSubTaskSplits);
		currentSubTaskSplits = new HashMap<>();
		final List<SplitT> checkpoint = new ArrayList<>();
		for (Map<Integer, List<SplitT>> subTaskSplits : subTaskSplitsPerCheckpoint.values()) {
			for (List<SplitT> splits : subTaskSplits.values()) {
				checkpoint.addAll(splits);
			}
		}
		return checkpoint;
	}

	void checkpointComplete(long checkpointId) throws IOException {
		final Iterator<Map.Entry<Long, Map<Integer, List<SplitT>>>> it =
			subTaskSplitsPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, Map<Integer, List<SplitT>>> entry = it.next();
			for (List<SplitT> splits : entry.getValue().values()) {
				splitCommitter.commit(splits);
			}
			it.remove();
		}
	}

	@Override
	public void close() throws Exception {
		splitCommitter.close();
	}
}
