package org.apache.flink.util.function;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.connector.sink.SplitSink;

/**
 * This is a wrapper function.
 */
public class SplitSinkCommitFunction<CommitT> implements CommitFunction<CommitT> {

	private final SplitSink<?, CommitT> splitSink;

	private transient CommitFunction<CommitT> commitFunction;

	public SplitSinkCommitFunction(SplitSink<?, CommitT> splitSink) {
		this.splitSink = splitSink;
	}

	@Override
	public void commit(CommitT commit) {
		//TODO:: maybe CommitFunction needs a open method
		if (commitFunction == null) {
			commitFunction = splitSink.createCommitFunction();
		}
		commitFunction.commit(commit);
	}
}
