package org.apache.flink.api.dag;

/**
 * Applies a Sink on the input {@link Transformation}.
 */
public abstract class Sink<InputT> extends TransformationApply<Transformation<InputT>, Transformation<?>> {
}
