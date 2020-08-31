package org.apache.flink.api.connector.sink;

/**
 * Clean up all resources ocuupied by the unmanaged committable data.
 * @param <CommT> The type of committable data.
 */
interface Cleaner<CommT> {

	void cleanUp(CommT commT);
}
