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
package org.apache.flink.test.state.operator.restore.chainunion;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.util.Utils;

/**
 * Verifies that the state of an operator is restored if it was previously not part of a chain
 */
public class ChainUnionTest extends AbstractOperatorRestoreTestBase {

	@Override
	protected void createOperators(DataStream<Integer> source) {
		SingleOutputStreamOperator<Integer> positiveMap = Utils.createPositiveMap(source, true);
		positiveMap.startNewChain();

		SingleOutputStreamOperator<Integer> negativeMap = Utils.createNegativeMap(positiveMap, true);
		// uncommenting the following line should cause the job to succeed
		//negativeMap.startNewChain();
	}

	@Override
	protected String getSavepointName() {
		return "savepoint-d582e6-590b8533be57";
	}
}
