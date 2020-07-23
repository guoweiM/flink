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

package org.apache.flink.streaming.api.functions.sink.filesystem.poc3;

import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.Serializable;
import java.util.List;

/**
 * TODO java doc.
 */
public class FileSinkSplit implements Serializable {

	public static FileSinkSplit DUMMY = new FileSinkSplit();

	/** this represents the data that could be committed when the checkpoint completes. */
	private final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables;

	/** this represents the data that could be cleanup when the checkpoint completes. */
	private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;

	/** for poc only */
	private final boolean isDummy;

	public FileSinkSplit(
		List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables,
		InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverables) {

		this.pendingFileRecoverables = pendingFileRecoverables;
		this.inProgressFileRecoverable = inProgressFileRecoverables;
		this.isDummy = false;
	}

	public FileSinkSplit() {
		this.isDummy = true;
		this.pendingFileRecoverables = null;
		this.inProgressFileRecoverable = null;
	}

	public List<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverables() {
		return pendingFileRecoverables;
	}

	public InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
		return inProgressFileRecoverable;
	}

	public boolean isDummy() {
		return isDummy;
	}
}
