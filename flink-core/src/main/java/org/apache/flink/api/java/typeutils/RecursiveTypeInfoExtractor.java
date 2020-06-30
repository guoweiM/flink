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
import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

class RecursiveTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	static final RecursiveTypeInfoExtractor INSTANCE = new RecursiveTypeInfoExtractor();

	@Override
	public Optional<TypeDescription> resolve(final Type type, final Context context) {
		if (isClassType(type)) {
			final Class<?> typeClass = typeToClass(type);
			if (countTypeInHierarchy(context.getExtractingClasses(), typeClass) > 1) {
				return Optional.of(new RecursiveTypeDescription(typeClass));
			}
		}
		return Optional.empty();
	}

	/**
	 * @return number of items with equal type or same raw type
	 */
	private static int countTypeInHierarchy(List<Class<?>> typeHierarchy, Type type) {
		int count = 0;
		for (Type t : typeHierarchy) {
			if (t == type || (isClassType(type) && t == typeToClass(type)) || (isClassType(t) && typeToClass(t) == type)) {
				count++;
			}
		}
		return count;
	}

	class RecursiveTypeDescription implements TypeDescription {

		private final Class<?> clazz;

		public RecursiveTypeDescription(final Class<?> clazz) {
			this.clazz = clazz;
		}

		@Override
		public TypeInformation<?> create() {
			return new GenericTypeInfo<>(clazz);
		}
	}
}
