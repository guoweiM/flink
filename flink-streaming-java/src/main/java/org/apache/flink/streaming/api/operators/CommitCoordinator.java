package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CommitCoordinator<CommitT> implements OperatorCoordinator {

	final Map<Integer, List<CommitT>> allFinalCommits = new HashMap<>();

	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		if (event instanceof FinalCommitsEvent) {
		} else {
			//TODO:: throw sth.
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {

	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
		resultFuture.complete(new byte[0]);
	}

	@Override
	public void checkpointComplete(long checkpointId) {

	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {

	}

	public static class FinalCommitsEvent<CommitT> implements OperatorEvent {

		final List<CommitT> commits;
		public FinalCommitsEvent(List<CommitT> commits) {
			this.commits = commits;
		}

		List<CommitT> getCommits() {
			return commits;
		}
	}
}
