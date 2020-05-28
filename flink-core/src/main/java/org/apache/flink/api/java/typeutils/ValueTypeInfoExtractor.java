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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Value;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Extract TypeInformation for Value type.
 */
public class ValueTypeInfoExtractor extends TypeInformationExtractorForClass {

	@Override
	public Optional<TypeInformation<?>> extract(Class<?> clazz) {
		if (Value.class.isAssignableFrom(clazz)) {
			Class<? extends Value> valueClass = clazz.asSubclass(Value.class);
			return Optional.of(ValueTypeInfo.getValueTypeInfo(valueClass));
		} else {
			return Optional.empty();
		}
	}

	@Override
	public List<Class<?>> getClasses() {
		return Collections.singletonList(Value.class);
	}
}
