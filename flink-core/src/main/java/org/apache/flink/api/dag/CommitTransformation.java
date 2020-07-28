package org.apache.flink.api.dag;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * A transformation that collects input "commits" and commits them at the "end" of the job, what
 * ever that means. I'm really just spitballing here and this is an example of a new custom {@link
 * Transformation} that we could introduce.
 */
public class CommitTransformation<CommitT> extends Transformation<CommitT> {

	private final Transformation<CommitT> input;

	private final CommitFunction<CommitT> commitFunction;
	private final TypeSerializer<CommitT> commitSerializer;

	/**
	 * Creates a new {@code CommitTransformation} that has the given input and uses the given {@link
	 * CommitFunction} for committing at the "end" of the job.
	 */
	public CommitTransformation(
			Transformation<CommitT> input,
			CommitFunction<CommitT> commitFunction,
			TypeSerializer<CommitT> commitSerializer) {
		super("Commit", input.getOutputType(), 1);
		this.input = input;
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
	}

	/**
	 * Returns the input {@code Transformation}.
	 */
	public Transformation<CommitT> getInput() {
		return input;
	}

	/**
	 * Returns the {@link CommitFunction} in this transformation.
	 */
	public CommitFunction<CommitT> getCommitFunction() {
		return commitFunction;
	}

	/**
	 * Returns a {@link TypeSerializer} for {@link CommitT}.
	 */
	public TypeSerializer<CommitT> getCommitSerializer() {
		return commitSerializer;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}
