package org.apache.flink.api.connector.init.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;
import java.util.Optional;

/**
 * This interface lets the sink developer to build a simple transactional sink topology pattern, which satisfies the HDFS/S3/Iceberg sink.
 * This sink topology includes one {@link Writer} + one {@link Committer} + one {@link GlobalCommitter}.
 * The {@link Writer} is responsible for producing the committable.
 * The {@link Committer} is responsible for committing a single committables.
 * The {@link GlobalCommitter} is responsible for committing an aggregated committable, which we called global committables.
 *
 * But both the {@link Committer} and the {@link GlobalCommitter} are optional.
 * @param <IN>
 * @param <CommT>
 * @param <GCommT>
 * @param <WriterS>
 */
interface TSink<IN, CommT, GCommT, WriterS> {

	Writer<IN, CommT, WriterS> createWriter(InitContext initContext);

	Writer<IN, CommT, WriterS> restoreWriter(InitContext initContext, List<WriterS> states);

	Optional<Committer<CommT>> createCommitter();

	Optional<GlobalCommitter<CommT, GCommT>> createGlobalCommitter();

	SimpleVersionedSerializer<CommT> getCommittableSerializer();

	Optional<SimpleVersionedSerializer<GCommT>> getGlobalCommittableSerializer();

	interface InitContext {

	}
}
