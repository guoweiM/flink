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

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;


/**
 * The interface for the split sink. It acts like a factory class that helps construct the {@link SplitSinkWriter}
 * and {@link CommitFunction}.
 * //TODO:: SplitSink extend Function just for extracting split TypeInformation. Maybe we could build it from scratch in the future.
 * @param <T>        The type of the sink's input.
 * @param <SplitT>   The type of data that is ready to be committed to the external system.
 */
@PublicEvolving
public interface SplitSink<T, SplitT> extends Function, Serializable {

	//TODO:: Do we really need a special SplitSinkWriter? or maybe the developer just need a Function that always has a output??
	//TODO:: Do we want the batch and streaming mode to the developer? There are many way to do it. Maybe we could materialized the committable stuff.
	//TODO:: How to clean up the Splits.
	SplitSinkWriter<T, SplitT> createWriter();

	CommitFunction<SplitT> createCommitFunction();

	//TODO:: create the SplitT serializer.
}
