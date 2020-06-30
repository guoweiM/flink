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
import org.apache.flink.api.java.typeutils.TypeDescriptionResolver.TypeDescription;

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
 * This class is used to extract {@link TupleTypeInfo}.
 */
public class TupleTypeInfoExtractor implements TypeInformationExtractor {

	@Override
	public List<Class<?>> getClasses() {
		return Collections.singletonList(Tuple.class);
	}

	public Optional<Type> resolve(final Type type, final ResolveContext context) {

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

		final Type[] types = new Type[resolvedType.getActualTypeArguments().length];
		for (int i = 0; i < typeArgumentsLength; i++) {
			types[i] = context.resolve(resolvedType.getActualTypeArguments()[i]);
		}
		return Optional.of(new TupleDescription(typeToClass(type), types));
	}

	private static class Tuple0Description extends TypeDescription {
		static final Tuple0Description INSTANCE = new Tuple0Description();

		static final TupleTypeInfo TUPLE0_TYPE_INFO = new TupleTypeInfo(Tuple0.class);

		@Override
		Type getType() {
			return Tuple0.class;
		}

		public TypeInformation<?> create() {
			return TUPLE0_TYPE_INFO;
		}
	}

	private static class TupleDescription extends TypeDescription {

		private final Type[] types;

		private final Class<?> clazz;

		TupleDescription(final Class<?> clazz, final Type[] typeDescriptions) {
			this.clazz = clazz;
			this.types = typeDescriptions;
		}

		public TypeInformation<?> create() {
			final int typeArgumentsLength = types.length;
			// create the type information for the subtypes
			final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];

			for (int i = 0; i < typeArgumentsLength; i++) {
				subTypesInfo[i] = ((TypeDescription) types[i]).create();
			}
			// return tuple info
			return new TupleTypeInfo(clazz, subTypesInfo);
		}

		public Type[] getTypes() {
			return types;
		}

		@Override
		Type getType() {
			return clazz;
		}

		public Class<?> getClazz() {
			return clazz;
		}
	}

	/**
	 * Extract the {@link TypeInformation} for the {@link Tuple}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or components.
	 * @return the {@link TypeInformation} of the type or {@link Optional#empty()} if the type information of the generic parameter of
	 * the {@link Tuple} could not be extracted
	 * @throws InvalidTypesException if the immediate child of {@link Tuple} is not a generic class or if the child of {@link Tuple}
	 * is not a generic class or if the type equals {@link Tuple}
	 */
	@SuppressWarnings("unchecked")
	public Optional<TypeInformation<?>> extract(final Type type, final Context context) {

		if (type instanceof Tuple0Description) {
			return Optional.of(((Tuple0Description) type).create());
		} else if (type instanceof TupleDescription) {
			return Optional.of(((TupleDescription) type).create());
		}
		return Optional.empty();
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
			final Optional<Type> curT =
				PojoTypeInfoExtractor.INSTANCE.resolve(value.getClass(),
					new TypeDescriptionResolver.TypeDescriptionResolveContext(Collections.singletonList(value.getClass()), Collections.emptyMap()));
			if (curT.isPresent()) {
				//TODO:: maybe we need a not empty current extracting class?? see
				return PojoTypeInfoExtractor.INSTANCE.extract(curT.get(), TypeInfoExtractContext.CONTEXT);
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
