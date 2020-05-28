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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;
import java.util.Optional;

class ArrayTypeInfoBinder {
	/**
	 * Bind the {@link TypeVariable}s in the given resolved array type with corresponding {@link TypeInformation}.
	 * This method infers {@link TypeVariable}'s {@link TypeInformation} in the component type through calling
	 * {@link TypeVariableBinder#bindTypeVariables}.
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@link Optional#empty()}
	 * if the type is not {@link GenericArrayType}
	 */
	static Optional<Map<TypeVariable<?>, TypeInformation<?>>> bindTypeVariables(
		final Type type,
		final TypeInformation<?> typeInformation) {

		if (type instanceof GenericArrayType) {
			TypeInformation<?> componentInfo = null;
			if (typeInformation instanceof BasicArrayTypeInfo) {
				componentInfo = ((BasicArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
			} else if (typeInformation instanceof PrimitiveArrayTypeInfo) {
				componentInfo = BasicTypeInfo.getInfoFor(typeInformation.getTypeClass().getComponentType());
			} else if (typeInformation instanceof ObjectArrayTypeInfo) {
				componentInfo = ((ObjectArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
			}
			Preconditions.checkNotNull(componentInfo, "found unexpected array type information");
			return Optional.of(TypeVariableBinder.bindTypeVariables(((GenericArrayType) type).getGenericComponentType(), componentInfo));
		}
		return Optional.empty();
	}
}
