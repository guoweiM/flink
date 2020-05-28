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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.TreeMap;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isAvroType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isHadoopWritable;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This class responses for finding extractor candidates.
 */
class TypeInformationExtractorFinder {

	/**
	 * 1. The registered classes are sorted as following :
	 * 	1.1 The child type is "greater" than the parent type. This is because we assume that the child type's extractor
	 *      is more suitable for a type.
	 * 	1.2 The sibling types uses name to compare.
	 * 2. Sorting by class is because that the finder returns a list candidates extractors that could
	 *    extract the given type's {@link TypeInformation}. We want want list has a deterministic order.
	 *
	 */
	private static final NavigableMap<Class<?>, TypeInformationExtractor> EXTRACTORS = new TreeMap<>((o1, o2) -> {
		Preconditions.checkArgument(o1 != null);
		Preconditions.checkArgument(o2 != null);
		boolean o2greater = o1.isAssignableFrom(o2);
		boolean o1greater = o2.isAssignableFrom(o1);

		if (o2greater && !o1greater) {
			return -1;
		} else if (o1greater && !o2greater) {
			return 1;
		}
		return o1.getName().compareTo(o2.getName());
	});


	/**
	 * Return a ordered list of {@link TypeInfoFactoryExtractor}s that might extract the given type's {@link TypeInformation}.
	 *
	 * @param type the given type
	 * @return {@link TypeInformationExtractor} or {@link Optional#empty()} if could not find any one.
	 * @throws RuntimeException if can not find {@link TypeInformationExtractor} for the Hadoop writable or the Avro class.
	 */
	static List<TypeInformationExtractor> findTypeInfoExtractor(final Type type) {

		loadExtractors();

		final List<TypeInformationExtractor> typeInfoExtractors = new ArrayList<>();

		typeInfoExtractors.add(TypeInfoFactoryExtractor.INSTANCE);

		if (isClassType(type)) {
			final Class<?> clazz = typeToClass(type);
			EXTRACTORS.descendingKeySet()
				.stream()
				.filter(c -> c.isAssignableFrom(clazz))
				.map(EXTRACTORS::get)
				.forEachOrdered(typeInfoExtractors::add);

			if (typeInfoExtractors.size() == 1) {
				if (isHadoopWritable(clazz)) {
					throw new RuntimeException("You may be missing the 'flink-hadoop-compatibility' dependency.");
				}

				if (isAvroType(clazz)) {
					throw new RuntimeException("You may be missing the 'flink-avro' dependency.");
				}
			}
		}

		typeInfoExtractors.add(ArrayTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(TypeVariableExtractor.INSTANCE);
		typeInfoExtractors.add(RecursiveTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(PojoTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(GenericTypeInfoExtractor.INSTANCE);

		return typeInfoExtractors;
	}

	@VisibleForTesting
	static NavigableMap<Class<?>, TypeInformationExtractor> getEXTRACTORS() {
		if (EXTRACTORS.size() == 0) {
			loadExtractors();
		}
		return EXTRACTORS;
	}

	private static void loadExtractors() {
		if (EXTRACTORS.size() == 0) {
			ServiceLoader
				.load(TypeInformationExtractor.class, TypeExtractor.class.getClassLoader())
				.forEach(typeInformationExtractor -> typeInformationExtractor.getClasses()
					.forEach(c -> EXTRACTORS.put(c, typeInformationExtractor))
				);
		}
	}

	private static class TypeVariableExtractor extends AutoRegisterDisabledTypeInformationExtractor {

		static final TypeVariableExtractor INSTANCE = new TypeVariableExtractor();

		@Override
		public Optional<TypeInformation<?>> extract(Type type, Context context) {
			if (type instanceof TypeVariable) {
				final TypeInformation<?> typeInfo = context.getTypeVariableBindings().get(type);
				if (typeInfo != null) {
					return Optional.of(typeInfo);
				} else {
					throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) type).getName() + "' in '"
						+ ((TypeVariable<?>) type).getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
						+ "The type extraction currently supports types with generic variables only in cases where "
						+ "all variables in the return type can be deduced from the input type(s). "
						+ "Otherwise the type has to be specified explicitly using type information.");
				}
			} else {
				return Optional.empty();
			}
		}
	}

	private static class RecursiveTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

		static final RecursiveTypeInfoExtractor INSTANCE = new RecursiveTypeInfoExtractor();

		@Override
		public Optional<TypeInformation<?>> extract(Type type, Context context) {
			if (isClassType(type)) {
				final Class<?> typeClass = typeToClass(type);
				if (countTypeInHierarchy(context.getExtractingClasses(), typeClass) > 1) {
					return Optional.of(new GenericTypeInfo<>(typeClass));
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
	}

	private static class GenericTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

		static final GenericTypeInfoExtractor INSTANCE = new GenericTypeInfoExtractor();

		@Override
		public Optional<TypeInformation<?>> extract(Type type, Context context) {
			if (type instanceof Class<?>) {
				return Optional.of(new GenericTypeInfo<>(typeToClass(type)));
			}
			return Optional.empty();
		}
	}
}
