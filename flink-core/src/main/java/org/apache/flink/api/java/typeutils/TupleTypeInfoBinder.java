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
import org.apache.flink.api.java.tuple.Tuple;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

class TupleTypeInfoBinder {
	/**
	 * Bind the {@link TypeVariable} in the resolved tuple type with corresponding {@link TypeInformation}.
	 * This method infers {@link TypeVariable}'s {@link TypeInformation} in every generic parameter in the tuple type through
	 * calling {@link TypeVariableBinder#bindTypeVariableFromGenericParameters}
	 *
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@link Optional#empty()} if the typeInformation
	 * is not {@link TupleTypeInfo}.
	 */
	static Optional<Map<TypeVariable<?>, TypeInformation<?>>> bindTypeVariables(final Type type, final TypeInformation<?> typeInformation) {

		if (typeInformation instanceof TupleTypeInfo && isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type))) {
			final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(type, Tuple.class);

			if (typeHierarchy.size() > 0) {
				final ParameterizedType tupleBaseClass = typeHierarchy.get(typeHierarchy.size() - 1);
				final ParameterizedType resolvedTupleBaseClass = (ParameterizedType) resolveTypeFromTypeHierarchy(tupleBaseClass, typeHierarchy, true);
				return Optional.of(TypeVariableBinder.bindTypeVariableFromGenericParameters(resolvedTupleBaseClass, typeInformation));
			}
			return Optional.of(Collections.emptyMap());
		}
		return Optional.empty();
	}
}
