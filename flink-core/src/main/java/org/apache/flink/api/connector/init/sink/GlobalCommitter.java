package org.apache.flink.api.connector.init.sink;

import java.util.List;

/**
 * The {@link GlobalCommitter} is responsible for committing an aggregated committable, which we called global committables.
 *
 * @param <CommT>
 * @param <GCommT>
 */
interface GlobalCommitter<CommT, GCommT> {

	/**
	 * This method is called when restoring from a failover.
	 * @param globalCommittables the global committables that are not committed in the previous session.
	 * @return the global committables that should be committed again in the current session.
	 */
	List<GCommT> filterRecoveredCommittables(List<GCommT> globalCommittables);

	/**
	 * Compute an aggregated committable from a collection of committables.
	 * @param committables a collection of committables that are needed to combine
	 * @return an aggregated committable
	 */
	GCommT combine(List<CommT> committables);

	void commit(List<GCommT> globalCommittables);

	/**
	 * There is no committables any more.
	 */
	void endOfInput();
}
