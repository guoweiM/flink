package org.apache.flink.streaming.api.dag;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.dag.TransformationApply;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

/**
 * Applies a {@link MapFunction} on the input {@link Transformation}.
 *
 * <p>TODO: This shouldn't be in flink-streaming-java but we currently need access to the physical
 * map operator here because we don't have a logical MapTransformation.
 */
public class Map<InputT, OutputT> extends TransformationApply<Transformation<InputT>, Transformation<OutputT>> {

	/**
	 * Creates a new {@link Map} from the given {@link MapFunction}.
	 */
	public static <InputT, OutputT> Map<InputT, OutputT> of(MapFunction<InputT, OutputT> mapFunction) {
		return new Map<>(mapFunction);
	}

	@Override
	public Transformation<OutputT> apply(Context context, Transformation<InputT> input) {
		MapFunction<InputT, OutputT> cleanedMapFunction = context.clean(mapFunction);

		TypeInformation<OutputT> outType = TypeExtractor.getMapReturnTypes(
				cleanedMapFunction,
				input.getOutputType());

		StreamMap<InputT, OutputT> mapOperator = new StreamMap<>(cleanedMapFunction);

		OneInputTransformation<InputT, OutputT> resultTransform = new OneInputTransformation<>(
				input,
				"Map",
				SimpleOperatorFactory.of(mapOperator),
				outType,
				input.getParallelism());

		return resultTransform;
	}

	private final MapFunction<InputT, OutputT> mapFunction;

	Map(MapFunction<InputT, OutputT> mapFunction) {
		this.mapFunction = mapFunction;
	}
}
