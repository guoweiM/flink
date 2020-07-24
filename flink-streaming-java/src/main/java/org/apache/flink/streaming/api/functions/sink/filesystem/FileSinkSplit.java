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

import java.util.List;

/**
 * TODO java doc.
 */
public class FileSinkSplit {

	/** this represents the data that could be committed when the checkpoint completes. */
	private final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables;

	/** this represents the data that could be cleanup when the checkpoint completes. */
	private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;

	public FileSinkSplit(
		List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables,
		InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverables) {

		this.pendingFileRecoverables = pendingFileRecoverables;
		this.inProgressFileRecoverable = inProgressFileRecoverables;
	}

	public List<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverables() {
		return pendingFileRecoverables;
	}

	public InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
		return inProgressFileRecoverable;
	}
}
