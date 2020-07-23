package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

public class CommitCoordinatorProvider extends RecreateOnResetOperatorCoordinator.Provider {

	public CommitCoordinatorProvider(OperatorID operatorID) {
		super(operatorID);
	}

	@Override
	protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
		return new CommitCoordinator();
	}
}
