package org.apache.flink.api.connector.init.sink;

interface Committer<CommT> {
	void commit(CommT commT);
}
