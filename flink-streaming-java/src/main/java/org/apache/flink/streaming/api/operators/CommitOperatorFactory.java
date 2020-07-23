package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

public class CommitOperatorFactory<CommitT> extends AbstractStreamOperatorFactory<Void>
	implements CoordinatedOperatorFactory<Void>{

	private final CommitFunction<CommitT> commitFunction;

	private final TypeSerializer<CommitT> commitSerializer;

	public CommitOperatorFactory(CommitFunction<CommitT> commitFunction, TypeSerializer<CommitT> commitSerializer) {
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new CommitCoordinatorProvider(operatorID);
	}

	@Override
	public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> parameters) {
		final OperatorID operatorId =
			parameters.getStreamConfig().getOperatorID();

		final CommitOperator<CommitT> commitOperator =
			new CommitOperator<>(commitFunction, commitSerializer, parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId));
		commitOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		return (T) commitOperator;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		//TODO:: fix
		return CommitOperator.class;
	}
}
