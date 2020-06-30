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

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 * This class is used to resolve the {@link TypeDescription} for the array type.
 */
class ArrayTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final ArrayTypeInfoExtractor INSTANCE = new ArrayTypeInfoExtractor();

	/**
	 * Compute the {@link TypeDescription} for the array type. There are two category array types: {@link GenericArrayType} and
	 * {@link Class#isArray()}.
	 * @param type the type needed to compute the {@link TypeDescription}.
	 * @param context used to resolve the {@link TypeDescription} for the generic parameters or components.
	 * @return the {@link TypeDescription} of the given type or {@link Optional#empty()} if the type is not an array type.
	 */
	@Override
	public Optional<TypeDescription> resolve(final Type type, final Context context) {
		if (type instanceof Class && ((Class<?>) type).isArray()) {
			return resolveArrayClass((Class<?>) type, context);
		} else if (type instanceof GenericArrayType) {
			return resolveGenericArray((GenericArrayType) type, context);
		}
		return Optional.empty();
	}

	static class PrimitiveArrayTypeDescription implements TypeDescription {

		private final Class<?> arrayClass;

		public PrimitiveArrayTypeDescription(Class<?> arrayClass) {
			this.arrayClass = arrayClass;
		}

		@Override
		public TypeInformation<?> create() {
			return PrimitiveArrayTypeInfo.getInfoFor(arrayClass);
		}
	}

	static class BasicArrayTypeDescription implements TypeDescription {

		private final Class<?> arrayClass;

		public BasicArrayTypeDescription(Class<?> arrayClass) {
			this.arrayClass = arrayClass;
		}

		@Override
		public TypeInformation<?> create() {
			return BasicArrayTypeInfo.getInfoFor(arrayClass);
		}
	}

	static class ObjectArrayTypeDescription implements TypeDescription {

		private final TypeDescription componentTypeDescription;

		public ObjectArrayTypeDescription(final TypeDescription componentTypeDescription) {
			this.componentTypeDescription = componentTypeDescription;
		}

		@Override
		public TypeInformation<?> create() {
			final TypeInformation<?> componentTypeInformation = componentTypeDescription.create();
			return ObjectArrayTypeInfo.getInfoFor(Array.newInstance(componentTypeInformation.getTypeClass(), 0).getClass(), componentTypeInformation);
		}
	}

	private Optional<TypeDescription> resolveArrayClass(final Class<?> clazz, final Context context) {
		if (PrimitiveArrayTypeInfo.getClasses().contains(clazz)) {
			return Optional.of(new PrimitiveArrayTypeDescription(clazz));
		}
		if (BasicArrayTypeInfo.getClasses().contains(clazz)) {
			return Optional.of(new BasicArrayTypeDescription(clazz));
		}
		final TypeDescription componentTypeDescription = context.resolve(((Class<?>) clazz).getComponentType());
		return Optional.of(new ObjectArrayTypeDescription(componentTypeDescription));
	}

	private Optional<TypeDescription> resolveGenericArray(final GenericArrayType genericArray, final Context context) {

		final Type componentType = genericArray.getGenericComponentType();
		if (componentType instanceof Class) {
			final Class<?> arrayClass;
			final Class<?> componentClass = (Class<?>) componentType;
			arrayClass = (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());
			return resolveArrayClass(arrayClass, context);
		} else {
			final TypeDescription componentTypeDescription = context.resolve(genericArray.getGenericComponentType());
			return Optional.of(new ObjectArrayTypeDescription(componentTypeDescription));
		}
	}
}
