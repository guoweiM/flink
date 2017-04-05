/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.test.state.operator.restore;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.state.operator.restore.util.Utils;
import org.junit.Test;

import java.net.URL;

public abstract class AbstractOperatorRestoreTestBase {
	@Test
	public void testRestore_1_2() throws Exception {
		testRestore("1.2_to_1.3");
	}

	@Test
	public void testRestore_1_3() throws Exception {
		testRestore("1.3");
	}

	private void testRestore(String flinkVersion) throws Exception {
		LocalStreamEnvironment env = (LocalStreamEnvironment) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Integer> source = Utils.createSource(env, true);

		createOperators(source);

		URL savepointResource = AbstractOperatorRestoreTestBase.class.getClassLoader().getResource("operatorstate/" + getSavepointName() + "/" + flinkVersion);
		if (savepointResource == null) {
			throw new IllegalArgumentException("Savepoint file does not exist.");
		}
		SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath(savepointResource.getFile(), true);

		env.execute("jobRestored", restoreSettings);

	}

	protected abstract void createOperators(DataStream<Integer> source);

	protected abstract String getSavepointName();
}
