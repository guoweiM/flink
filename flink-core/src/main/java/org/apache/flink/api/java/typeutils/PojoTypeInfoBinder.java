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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getAllDeclaredFields;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

class PojoTypeInfoBinder {

	/**
	 * Bind the {@link TypeVariable} in the resolved pojo type with corresponding {@link TypeInformation}.
	 * This method infers {@link TypeVariable}'s {@link TypeInformation} in every field of the type through calling
	 * {@link TypeVariableBinder#bindTypeVariables}.
	 *
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@link Optional#empty()} if the
	 * typeInformation is not {@link PojoTypeInfo}.
	 */
	static Optional<Map<TypeVariable<?>, TypeInformation<?>>> bindTypeVariables(
		final Type type,
		final TypeInformation<?> typeInformation) {

		if (typeInformation instanceof PojoTypeInfo && isClassType(type)) {
			final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();
			// build the entire type hierarchy for the pojo
			final List<ParameterizedType> pojoHierarchy = buildParameterizedTypeHierarchy(type, Object.class);
			// build the entire type hierarchy for the pojo
			final List<Field> fields = getAllDeclaredFields(typeToClass(type), false);
			for (Field field : fields) {
				final Type fieldType = field.getGenericType();
				final Type resolvedFieldType = resolveTypeFromTypeHierarchy(fieldType, pojoHierarchy, true);
				final Map<TypeVariable<?>, TypeInformation<?>> sub =
					TypeVariableBinder.bindTypeVariables(resolvedFieldType, getTypeOfPojoField(typeInformation, field));
				typeVariableBindings.putAll(sub);
			}

			return Optional.of(typeVariableBindings);
		}
		return Optional.empty();
	}

	static TypeInformation<?> getTypeOfPojoField(TypeInformation<?> pojoInfo, Field field) {
		for (int j = 0; j < pojoInfo.getArity(); j++) {
			PojoField pf = ((PojoTypeInfo<?>) pojoInfo).getPojoFieldAt(j);
			if (pf.getField().getName().equals(field.getName())) {
				return pf.getTypeInformation();
			}
		}
		throw new InvalidTypesException("this is an invalid pojo type information");
	}
}
