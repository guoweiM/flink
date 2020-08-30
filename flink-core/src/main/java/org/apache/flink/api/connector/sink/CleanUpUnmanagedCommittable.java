package org.apache.flink.api.connector.sink;

public interface CleanUpUnmanagedCommittable {

	/**
	 * Notify the sink that all the managed committable before this session has been committed.
	 * Then writer can clean up all the un-managed committable that does not belongs to current session.
	 */
	void cleanUp();
}
