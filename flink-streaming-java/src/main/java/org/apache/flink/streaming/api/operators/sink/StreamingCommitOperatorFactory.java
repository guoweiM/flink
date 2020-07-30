package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * Java doc.
 * @param <CommitT>
 */
public class StreamingCommitOperatorFactory<CommitT> extends AbstractStreamOperatorFactory<Void> {

	private final CommitFunction<CommitT> commitFunction;

	private final TypeSerializer<CommitT> commitSerializer;

	public StreamingCommitOperatorFactory(CommitFunction<CommitT> commitFunction, TypeSerializer<CommitT> commitSerializer) {
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
	}

	@Override
	public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> parameters) {
		final OperatorID operatorId =
			parameters.getStreamConfig().getOperatorID();

		final StreamingCommitOperator<CommitT> commitOperator =
			new StreamingCommitOperator<>(commitFunction, commitSerializer);
		commitOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		return (T) commitOperator;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return StreamingCommitOperator.class;
	}
}
