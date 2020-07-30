package org.apache.flink.api.connector.sink;

import org.apache.flink.util.AbstractID;

public interface CommitListener {

	/**
	 * Notify the writer that all the managed committables before this session has been committed.
	 * Then writer can clean up all the un-manged comittables that does not belongs to current session.
	 * @param sessionID
	 */
	void commitsFinished(AbstractID sessionID);
}
