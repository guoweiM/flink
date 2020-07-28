package org.apache.flink.streaming.api.dag;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.CommitTransformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.dag.TransformationApply;

/**
 * Applies a {@link CommitFunction} on the input {@link Transformation}.
 *
 * <p>TODO: This shouldn't be in flink-streaming-java but we currently need access to the physical
 * map operator here because we don't have a logical MapTransformation.
 */
public class Commit<CommitT> extends TransformationApply<Transformation<CommitT>, Transformation<Void>> {

	/**
	 * Creates a new {@link Commit} from the given {@link MapFunction}.
	 */
	public static <InputT> Commit<InputT> of(CommitFunction<InputT> commitFunction) {
		return new Commit<>(commitFunction);
	}

	@Override
	public Transformation<Void> apply(Context context, Transformation<CommitT> input) {
		CommitFunction<CommitT> cleanedCommitFunction = context.clean(commitFunction);

		CommitTransformation<CommitT> commitTransformation = new CommitTransformation<>(
				input,
				cleanedCommitFunction);

		return commitTransformation;
	}

	private final CommitFunction<CommitT> commitFunction;

	Commit(CommitFunction<CommitT> commitFunction) {
		this.commitFunction = commitFunction;
	}
}
