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
 * This class is used to extract {@link TypeInformation} for the array type. There are two category array types:
 * {@link GenericArrayType} and {@link Class#isArray()}.
 */
class ArrayTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final ArrayTypeInfoExtractor INSTANCE = new ArrayTypeInfoExtractor();

	public Optional<Type> resolve(final Type type, final ResolveContext context) {
		if (type instanceof Class && ((Class<?>) type).isArray()) {
			return resolveArrayClass((Class<?>) type, context);
		} else if (type instanceof GenericArrayType) {
			return resolveGenericArray((GenericArrayType) type, context);
		}
		return Optional.empty();
	}

	abstract static class ArrayClassDescription extends TypeDescriptionResolver.TypeDescription {
		private final Class<?> arrayClass;

		public ArrayClassDescription(final Class<?> arrayClass) {
			this.arrayClass = arrayClass;
		}

		public Class<?> getArrayClass() {
			return arrayClass;
		}

		public Type getType() {
			return arrayClass;
		}
	}

	static class PrimitiveArrayTypeDescription extends ArrayClassDescription {

		public PrimitiveArrayTypeDescription(Class<?> arrayClass) {
			super(arrayClass);
		}
	}

	static class BasicArrayTypeDescription extends  ArrayClassDescription {

		public BasicArrayTypeDescription(Class<?> arrayClass) {
			super(arrayClass);
		}
	}

	static class ObjectArrayTypeDescription extends TypeDescriptionResolver.TypeDescription {

		private final Type componentType;

		private final Type type;

		public ObjectArrayTypeDescription(final Type type, final Type componentType) {
			this.type = type;
			this.componentType = componentType;
		}

		public Type getComponentType() {
			return componentType;
		}

		@Override
		Type getType() {
			return type;
		}
	}

	private Optional<Type> resolveArrayClass(final Class<?> clazz, final ResolveContext context) {
		if (PrimitiveArrayTypeInfo.getClasses().contains(clazz)) {
			return Optional.of(new PrimitiveArrayTypeDescription(clazz));
		}
		if (BasicArrayTypeInfo.getClasses().contains(clazz)) {
			return Optional.of(new BasicArrayTypeDescription(clazz));
		}
		final Type componentTypeDescription = context.resolve(((Class<?>) clazz).getComponentType());
		return Optional.of(new ObjectArrayTypeDescription(clazz, componentTypeDescription));
	}

	private Optional<Type> resolveGenericArray(final GenericArrayType genericArray, final ResolveContext context) {

		final Type componentType = genericArray.getGenericComponentType();
		if (componentType instanceof Class) {
			final Class<?> arrayClass;
			final Class<?> componentClass = (Class<?>) componentType;
			arrayClass = (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());
			return resolveArrayClass(arrayClass, context);
		} else {
			final Type componentTypeDescription = context.resolve(genericArray.getGenericComponentType());
			return Optional.of(new ObjectArrayTypeDescription(genericArray, componentTypeDescription));
		}
	}

	/**
	 * Extract {@link TypeInformation} for the array type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or components.
	 * @return the {@link TypeInformation} of the given type or {@link Optional#empty()} if the type is not an array type.
	 */
	public Optional<TypeInformation<?>> extract(final Type type, final TypeInformationExtractor.Context context) {

		if (type instanceof PrimitiveArrayTypeDescription) {
			final Class<?> clazz = ((PrimitiveArrayTypeDescription) type).getArrayClass();
			return Optional.of(PrimitiveArrayTypeInfo.getInfoFor(clazz));
		} else if (type instanceof BasicArrayTypeDescription) {
			final Class<?> clazz = ((BasicArrayTypeDescription) type).getArrayClass();
			return Optional.of(BasicArrayTypeInfo.getInfoFor(clazz));
		} else if (type instanceof ObjectArrayTypeDescription) {
			final TypeInformation<?> componentTypeInformation =
				context.extract(((ObjectArrayTypeDescription) type).getComponentType());
			return Optional.of(ObjectArrayTypeInfo.getInfoFor(Array.newInstance(componentTypeInformation.getTypeClass(), 0).getClass(), componentTypeInformation));
		}
		return Optional.empty();
	}
}
