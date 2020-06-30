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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.InstantiationUtil;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

/**
 * This class is used to compute the {@link TypeInfoFactoryDescriptor} for the class that has {@link TypeInfo} annotation in
 * it's type hierarchy.
 */
public class TypeInfoFactoryExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final TypeInfoFactoryExtractor INSTANCE = new TypeInfoFactoryExtractor();

	/**
	 * Find the first type that has the {@link TypeInfo} annotation in the type hierarchy of given type. Then create a
	 * {@link TypeInfoFactoryDescriptor} according to its generic parameters.
	 * @param type  the type needed to compute a {@link TypeDescription}
	 * @param context used to resolve the {@link TypeDescription} for the generic parameters or components.
	 * @return the {@link TypeInfoFactoryDescriptor} if finding a type that has the annotation or {@link Optional#empty()} if can not find one.
	 * @throws InvalidTypesException if error occurs when resolving the generic parameter.
	 */
	public Optional<TypeDescription> resolve(final Type type, final Context context) {
		final Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> result = buildTypeHierarchy(type);

		if (result == null) {
			return Optional.empty();
		}

		final Type resolvedFactoryDefiningType = resolveTypeFromTypeHierarchy(result.f0, result.f1, true);
		final Map<String, TypeDescription> genericParams;

		if (resolvedFactoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final Type[] genericTypes = ((ParameterizedType) resolvedFactoryDefiningType).getActualTypeArguments();
			final Type[] args = typeToClass(resolvedFactoryDefiningType).getTypeParameters();
			for (int i = 0; i < genericTypes.length; i++) {
				genericParams.put(args[i].toString(),  context.resolve(genericTypes[i]));
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		return Optional.of(new TypeInfoFactoryDescriptor(typeToClass(type), result.f2, genericParams));
	}

	static class TypeInfoFactoryDescriptor implements TypeDescription {

		private final Map<String, TypeDescription> genericParams;

		private final TypeInfoFactory<?> typeInfoFactory;

		private final Class<?> clazz;

		public TypeInfoFactoryDescriptor(final Class<?> clazz, final TypeInfoFactory<?> typeInfoFactory, final Map<String, TypeDescription> genericParams) {
			this.clazz = clazz;
			this.typeInfoFactory = typeInfoFactory;
			this.genericParams = genericParams;
		}

		@Override
		public TypeInformation<?> create() {

			final Map<String, TypeInformation<?>> typeInformationOfGenericParams;

			if (!genericParams.isEmpty()) {
				typeInformationOfGenericParams = new HashMap<>();
				for (String name : genericParams.keySet()) {
					typeInformationOfGenericParams.put(name, genericParams.get(name).create());
				}
			} else {
				typeInformationOfGenericParams = Collections.emptyMap();
			}
			final TypeInformation<?> createdTypeInfo = typeInfoFactory.createTypeInfo(clazz, typeInformationOfGenericParams);
			if (createdTypeInfo == null) {
				throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
			}
			return createdTypeInfo;
		}
	}

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	@Internal
	@SuppressWarnings("unchecked")
	public static <X> TypeInfoFactory<X> getTypeInfoFactory(Type t) {
		final Class<?> factoryClass;

		if (!isClassType(t) || !typeToClass(t).isAnnotationPresent(TypeInfo.class)) {
			return null;
		}
		final TypeInfo typeInfoAnnotation = typeToClass(t).getAnnotation(TypeInfo.class);
		factoryClass = typeInfoAnnotation.value();
		// check for valid factory class
		if (!TypeInfoFactory.class.isAssignableFrom(factoryClass)) {
			throw new InvalidTypesException("TypeInfo annotation does not specify a valid TypeInfoFactory.");
		}
		// instantiate
		return (TypeInfoFactory<X>) InstantiationUtil.instantiate(factoryClass);
	}

	static Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> buildTypeHierarchy(final Type type) {

		if (!isClassType(type)) {
			return null;
		}
		final List<ParameterizedType> factoryHierarchy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(
			type,
			clazz -> clazz.equals(Object.class) || getTypeInfoFactory(clazz) != null,
			clazz -> true);

		final TypeInfoFactory<?> factory;
		final Type factoryDefiningType;

		if (factoryHierarchy.size() > 0) {
			factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);
			factory = getTypeInfoFactory(factoryDefiningType);
		} else {
			factoryDefiningType = type;
			factory = getTypeInfoFactory(type);
		}

		if (factory == null) {
			return null;
		}

		return Tuple3.of(factoryDefiningType, factoryHierarchy, factory);
	}

}
