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
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 * This class is used to extract {@link TypeInformation} for the array type. There are two category array types:
 * {@link GenericArrayType} and {@link Class#isArray()}. This class would use the
 * {@link #extractTypeInformationForGenericArray(Type, TypeInformationExtractor.Context)} and
 * {@link #extractTypeInformationForClassArray(Type, TypeInformationExtractor.Context)} to extract the {@link TypeInformation}
 * for the array type separately.
 */
class ArrayTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final ArrayTypeInfoExtractor INSTANCE = new ArrayTypeInfoExtractor();

	/**
	 * Extract {@link TypeInformation} for the array type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or components.
	 * @return the {@link TypeInformation} of the given type or {@link Optional#empty()} if the type is not an array type.
	 */
	public Optional<TypeInformation<?>> extract(final Type type, final TypeInformationExtractor.Context context) {

		TypeInformation<?> typeInformation = extractTypeInformationForGenericArray(type, context);
		if (typeInformation != null) {
			return Optional.of(typeInformation);
		}
		return Optional.ofNullable(extractTypeInformationForClassArray(type, context));
	}

	@Nullable
	private static TypeInformation<?> extractTypeInformationForGenericArray(final Type type, final TypeInformationExtractor.Context context) {

		final Class<?> classArray;

		if (type instanceof GenericArrayType) {
			final GenericArrayType genericArray = (GenericArrayType) type;

			final Type componentType = ((GenericArrayType) type).getGenericComponentType();
			if (componentType instanceof Class) {
				final Class<?> componentClass = (Class<?>) componentType;

				classArray = (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());
				return context.extract(classArray);
			} else {
				final TypeInformation<?> componentInfo = context.extract(genericArray.getGenericComponentType());
				return ObjectArrayTypeInfo.getInfoFor(
					Array.newInstance(componentInfo.getTypeClass(), 0).getClass(), componentInfo);
			}
		}
		return null;
	}

	@Nullable
	private static TypeInformation<?> extractTypeInformationForClassArray(final Type type, final TypeInformationExtractor.Context context) {

		if (type instanceof Class && ((Class) type).isArray()) {
			final Class<?> classArray = (Class<?>) type;
			// primitive arrays: int[], byte[], ...
			final PrimitiveArrayTypeInfo<?> primitiveArrayInfo = PrimitiveArrayTypeInfo.getInfoFor(classArray);
			if (primitiveArrayInfo != null) {
				return primitiveArrayInfo;
			}

			// basic type arrays: String[], Integer[], Double[]
			final BasicArrayTypeInfo<?, ?> basicArrayInfo = BasicArrayTypeInfo.getInfoFor(classArray);
			if (basicArrayInfo != null) {
				return basicArrayInfo;
			} else {
				final TypeInformation<?> componentTypeInfo = context.extract(classArray.getComponentType());
				return ObjectArrayTypeInfo.getInfoFor(classArray, componentTypeInfo);
			}
		}
		return null;
	}

}
