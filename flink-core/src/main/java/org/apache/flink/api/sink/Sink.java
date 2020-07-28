package org.apache.flink.api.sink;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.dag.TransformationApply;

/**
 * A sink.
 */
public abstract class Sink<InputT> extends TransformationApply<Transformation<InputT>, Transformation<?>> {
}
