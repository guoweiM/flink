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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

/**
 * This class is used to resolve {@link TupleDescription}.
 */
public class TupleTypeInfoExtractor implements TypeInformationExtractor {

	@Override
	public List<Class<?>> getClasses() {
		return Collections.singletonList(Tuple.class);
	}

	/**
	 * Resolve the {@link TupleDescription} for the {@link Tuple}.
	 * @param type the type needed to compute {@link TypeDescription}.
	 * @param context used to resolve the {@link TypeDescription} for the generic parameters or components.
	 * @return the {@link TupleDescription} or {@link Optional#empty()} if the given type is not a Tuple.
	 * @throws InvalidTypesException if the immediate child of {@link Tuple} is not a generic class or if the child of {@link Tuple}
	 * is not a generic class or if the type equals {@link Tuple}
	 */
	public Optional<TypeDescription> resolve(final Type type, final Context context) {

		if (!(isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type)))) {
			return Optional.empty();
		}

		//do not allow usage of Tuple as type
		if (typeToClass(type).equals(Tuple.class)) {
			throw new InvalidTypesException(
				"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
		}

		if (Tuple0.class.isAssignableFrom(typeToClass(type))) {
			return Optional.of(Tuple0Description.INSTANCE);
		}

		final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(type, Tuple.class);

		if (typeHierarchy.size() < 1) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		// check if immediate child of Tuple has generics
		// TODO:: add a test for it
		if (typeToClass(typeHierarchy.get(typeHierarchy.size() - 1)).getSuperclass() != Tuple.class) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		final ParameterizedType curT = typeHierarchy.get(typeHierarchy.size() - 1);
		final int typeArgumentsLength = curT.getActualTypeArguments().length;

		// check the origin type contains additional fields.
		if (countFieldsInClass(typeToClass(type)) > typeArgumentsLength) {
			return Optional.empty();
		}
		final ParameterizedType resolvedType = (ParameterizedType) resolveTypeFromTypeHierarchy(curT, typeHierarchy, true);

		final TypeDescription[] types = new TypeDescription[resolvedType.getActualTypeArguments().length];
		for (int i = 0; i < typeArgumentsLength; i++) {
			types[i] = context.resolve(resolvedType.getActualTypeArguments()[i]);
		}
		return Optional.of(new TupleDescription(typeToClass(type), types));
	}

	private static class Tuple0Description implements TypeDescription {
		static final Tuple0Description INSTANCE = new Tuple0Description();

		static final TupleTypeInfo TUPLE0_TYPE_INFO = new TupleTypeInfo(Tuple0.class);

		@Override
		public TypeInformation<?> create() {
			return TUPLE0_TYPE_INFO;
		}
	}

	private static class TupleDescription implements TypeDescription {

		private final TypeDescription[] types;

		private final Class<?> clazz;

		TupleDescription(final Class<?> clazz, final TypeDescription[] typeDescriptions) {
			this.clazz = clazz;
			this.types = typeDescriptions;
		}

		@Override
		public TypeInformation<?> create() {
			final int typeArgumentsLength = types.length;
			// create the type information for the subtypes
			final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];

			for (int i = 0; i < typeArgumentsLength; i++) {
				subTypesInfo[i] = types[i].create();
			}
			// return tuple info
			return new TupleTypeInfo(clazz, subTypesInfo);
		}
	}

	/**
	 * Extract the {@link TypeInformation} for the Tuple object.
	 * @param value the object needed to extract {@link TypeInformation}
	 * @return the {@link TypeInformation} of the type or {@code null} if the value is not the Tuple type.
	 */
	@SuppressWarnings("unchecked")
	//TODO:: move this method out or make it a interface
	static Optional<TypeInformation<?>> extract(Object value) {

		if (!(value instanceof Tuple)) {
			return Optional.empty();
		}

		final Tuple t = (Tuple) value;
		final int numFields = t.getArity();

		if (numFields != countFieldsInClass(value.getClass())) {
			// not a tuple since it has more fields.
			// we immediately call analyze Pojo here, because there is currently no other type that can handle such a class.
			//TODO:: use createType??
			final Optional<TypeDescription> curT =
				PojoTypeInfoExtractor.INSTANCE.resolve(value.getClass(),
					new TypeDescriptionContext(Collections.singletonList(value.getClass()), Collections.emptyMap()));
			if (curT.isPresent()) {
				//TODO:: maybe we need a not empty current extracting class?? see
				return Optional.of(curT.get().create());
			} else {
				return Optional.empty();
			}
		}

		final TypeInformation<?>[] typeInformations = new TypeInformation[numFields];

		for (int i = 0; i < numFields; i++) {
			Object field = t.getField(i);

			if (field == null) {
				throw new InvalidTypesException("Automatic type extraction is not possible on candidates with null values. "
					+ "Please specify the types directly.");
			}

			typeInformations[i] = getForObject(field);
		}
		return Optional.of(new TupleTypeInfo(value.getClass(), typeInformations));
	}

	private static int countFieldsInClass(Class<?> clazz) {
		int fieldCount = 0;
		for (Field field : clazz.getFields()) { // get all fields
			if (!Modifier.isStatic(field.getModifiers()) &&
				!Modifier.isTransient(field.getModifiers())
			) {
				fieldCount++;
			}
		}
		return fieldCount;
	}
}
