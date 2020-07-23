package org.apache.flink.streaming.api.functions.sink.filesystem.poc3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.operators.BoundedOneInput;

public class FileSinkFunction<IN> extends RichMapFunction<IN, FileSinkSplit> implements CheckpointedFunction, BoundedOneInput {


	private final StreamingFileSink.BucketsBuilder<IN, ?, ? extends StreamingFileSink.BucketsBuilder<IN, ?, ?>> bucketsBuilder;

	private transient FileSinkWriter<IN, ?> fileSinkWriter;

	public FileSinkFunction(
		StreamingFileSink.BucketsBuilder<IN, ?, ? extends StreamingFileSink.BucketsBuilder<IN, ?, ?>> bucketsBuilder) {
		this.bucketsBuilder = bucketsBuilder;
	}


	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		fileSinkWriter.persist();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		fileSinkWriter =
			new FileSinkWriter(
				getRuntimeContext().getIndexOfThisSubtask(),
				context.isRestored(), context.getOperatorStateStore(),
				this.bucketsBuilder, 10);
	}

	@Override
	public FileSinkSplit map(IN value) throws Exception {
		//TODO:: Maybe we could let writer return FileSinkSplit
		fileSinkWriter.write(value);
		return FileSinkSplit.DUMMY;
	}

	@Override
	public void endInput() throws Exception {
		//TODO:: flush all the commit to the downstream.
	}
}
