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

import java.lang.reflect.Type;
import java.util.Optional;

/**
 * The extractor extending this class resolves {@link TypeDescription} of non generic parameter class and also does not need to
 * resolve the field's {@link TypeDescription}.
 */
public abstract class TypeInformationExtractorForClass implements TypeInformationExtractor {

	@Override
	public Optional<TypeDescription> resolve(final Type type, final Context context) {
		if (type instanceof Class) {
			return resolve((Class<?>)type);
		}
		return Optional.empty();
	}

	/**
	 * The descriptor is used to create non generic parameter class's {@link TypeInformation}.
	 */
	public abstract class ClassDescription implements TypeDescription {

		private final Class<?> clazz;

		public ClassDescription(final Class<?> clazz) {
			this.clazz = clazz;
		}

		public Class<?> getClazz() {
			return clazz;
		}

		@Override
		public abstract TypeInformation<?> create();
	}

	public abstract Optional<TypeDescription> resolve(final Class<?> clazz);

}
