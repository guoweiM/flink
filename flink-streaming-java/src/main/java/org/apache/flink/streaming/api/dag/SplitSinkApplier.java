package org.apache.flink.streaming.api.dag;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.dag.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

/**
 * This {@link org.apache.flink.api.dag.TransformationApply} is responsible for converting a {@link USink} to a
 * transformation topology. //TODO:: This should not be here.
 * @param <InputT> The type of sink's input.
 * @param <SplitT> The type of data that is ready to be committed to the external system.
 */
public class SplitSinkApplier<InputT, SplitT> extends Sink<InputT> {

	//TODO:: following could be initialized form other transformation such as SplitSinkTransformation

	private final USink<InputT, SplitT> uSink;

	private final String name;

	private final int parallelism;

	private final TypeInformation<SplitT> splitTypeInformation;

	@Override
	public Transformation<Void> apply(Context context, Transformation<InputT> input) {
		// TODO:: Maybe we could use different operator for this according to batch or streaming execution mode.
		// I have to use the operator directly here. It is because that sink needs to sending the split at the checkpoint time.
		final OneInputTransformation<InputT, SplitT> oneInputTransformation =
			new OneInputTransformation<>(input, name, new SinkWriterOperatorFactory<>(uSink), splitTypeInformation, parallelism);

		return oneInputTransformation.apply(context, Commit.of(uSink.createCommitFunction()));
	}

	SplitSinkApplier(USink<InputT, SplitT> uSink) {
		this.uSink = uSink;
		this.name = "split-sink";
		this.parallelism = 4;
		this.splitTypeInformation = null;
	}
}
