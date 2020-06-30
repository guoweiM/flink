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

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Resolve {@link SQLTimeTypeDescription} for SQL Time Type.
 */
public class SQLTimeTypeInfoExtractor extends TypeInformationExtractorForClass {

	private static final List<Class<?>> CLASS_LIST = Arrays.asList(Date.class, Time.class, Timestamp.class);

	@Override
	public Optional<TypeDescription> resolve(final Class<?> clazz) {
		if (CLASS_LIST.contains(clazz)) {
			return Optional.of(new SQLTimeTypeDescription(clazz));
		}
		return Optional.empty();
	}

	@Override
	public List<Class<?>> getClasses() {
		return CLASS_LIST;
	}

	class SQLTimeTypeDescription extends ClassDescription {

		public SQLTimeTypeDescription(final Class<?> clazz) {
			super(clazz);
		}

		@Override
		public TypeInformation<?> create() {
			return SqlTimeTypeInfo.getInfoFor(getClazz());
		}
	}
}
