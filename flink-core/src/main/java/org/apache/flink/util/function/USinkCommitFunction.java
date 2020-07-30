package org.apache.flink.util.function;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.connector.sink.USink;

/**
 * This is a wrapper function.
 */
public class USinkCommitFunction<CommitT> implements CommitFunction<CommitT> {

	private final USink<?, CommitT> uSink;

	private transient CommitFunction<CommitT> commitFunction;

	public USinkCommitFunction(USink<?, CommitT> uSink) {
		this.uSink = uSink;
	}

	@Override
	public void commit(CommitT commit) {
		//TODO:: maybe CommitFunction needs a open method
		if (commitFunction == null) {
			commitFunction = uSink.createCommitFunction();
		}
		commitFunction.commit(commit);
	}
}
