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
package org.apache.flink.test.state.operator.restore.chainlength;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.util.Utils;

/**
 * Verifies that the satte of an operator is correctly restored if the length of the contained chain changes.
 */
public class ChainLengthDecreaseTest extends AbstractOperatorRestoreTestBase {

	@Override
	protected void createOperators(DataStream<Integer> source) {
		// adding this operator before negativeMap should cause the job to succeed
		//SingleOutputStreamOperator<Integer> positiveMap = Utils.createPositiveMap(source, restored);
		//positiveMap.startNewChain();

		SingleOutputStreamOperator<Integer> negativeMap = Utils.createNegativeMap(source, true);
		negativeMap.startNewChain();
	}

	@Override
	protected String getSavepointName() {
		return "savepoint-688e80-7dd67df6a924";
	}
}
