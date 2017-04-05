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
package org.apache.flink.test.state.operator.restore.chainbreak;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.util.Utils;

import java.net.URL;

public class ChainBreakSavepointMigrator {
	public static void main(String[] args) throws Exception {
		boolean restored = false;

		ParameterTool pt = ParameterTool.fromArgs(args);

		String savepointsPath = pt.getRequired("savepoint-path");

		Configuration config = new Configuration();
		config.setString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY, savepointsPath);

		LocalStreamEnvironment env = (LocalStreamEnvironment) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());

		env.setStateBackend(new MemoryStateBackend());

		DataStream<Integer> source = Utils.createSource(env, restored);

		SingleOutputStreamOperator<Integer> positiveMap = Utils.createPositiveMap(source, restored);
		positiveMap.startNewChain();

		SingleOutputStreamOperator<Integer> negativeMap = Utils.createNegativeMap(positiveMap, restored);

		URL savepointResource = AbstractOperatorRestoreTestBase.class.getClassLoader().getResource("operatorstate/chainBreak/1.2");
		if (savepointResource == null) {
			throw new IllegalArgumentException("Savepoint file does not exist.");
		}

		env.execute("job", SavepointRestoreSettings.forPath(savepointResource.getFile()));
	}
}
