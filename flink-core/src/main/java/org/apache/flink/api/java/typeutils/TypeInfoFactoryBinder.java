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

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeVariableBinder.bindTypeVariableFromGenericParameters;

class TypeInfoFactoryBinder {
	/**
	 * Bind the {@link TypeVariable}s in the given resolved type with corresponding {@link TypeInformation}.
	 * This method infers {@link TypeVariable}'s {@link TypeInformation} in very generic parameter in the given type through
	 * calling {@link TypeVariableBinder#bindTypeVariableFromGenericParameters}
	 *
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between the {@link TypeVariable} and {@link TypeInformation} or {@link Optional#empty()}
	 * if the typeInformation is not created from the {@link TypeInfoFactory}
	 */
	static Optional<Map<TypeVariable<?>, TypeInformation<?>>> bindTypeVariables(final Type type, final TypeInformation<?> typeInformation) {

		final Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> result = TypeInfoFactoryExtractor.buildTypeHierarchy(type);

		if (result == null) {
			return Optional.empty();
		}
		if (result.f0 instanceof ParameterizedType) {
			final ParameterizedType resolvedFactoryDefiningType =
				(ParameterizedType) resolveTypeFromTypeHierarchy(result.f0, result.f1, true);
			return Optional.of(bindTypeVariableFromGenericParameters(resolvedFactoryDefiningType, typeInformation));
		} else {
			return Optional.of(Collections.emptyMap());
		}
	}
}
