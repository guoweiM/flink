package org.apache.flink.api.connector.init.sink;

import java.util.List;

interface Writer<IN, CommT, StateT> {

	void write(IN element, Context ctx, WriterOutput<CommT> output);

	void prepareCommit(boolean flush, WriterOutput<CommT> output);

	List<StateT> snapshotState(WriterOutput<CommT> output);

	interface Context {

	}
}
