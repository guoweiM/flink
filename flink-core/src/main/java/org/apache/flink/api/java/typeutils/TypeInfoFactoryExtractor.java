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
 * This class is used to extract the {@link org.apache.flink.api.common.typeinfo.TypeInformation} of the class that has
 * the {@link org.apache.flink.api.common.typeinfo.TypeInfo} annotation.
 */
public class TypeInfoFactoryExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final TypeInfoFactoryExtractor INSTANCE = new TypeInfoFactoryExtractor();

	public Optional<Type> resolve(final Type type, final ResolveContext context) {
		final Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> result = buildTypeHierarchy(type);

		if (result == null) {
			return Optional.empty();
		}

		final Type resolvedFactoryDefiningType = resolveTypeFromTypeHierarchy(result.f0, result.f1, true);
		final Map<String, Type> genericParams;

		if (resolvedFactoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final Type[] genericTypes = ((ParameterizedType) resolvedFactoryDefiningType).getActualTypeArguments();
			final Type[] args = typeToClass(resolvedFactoryDefiningType).getTypeParameters();
			for (int i = 0; i < genericTypes.length; i++) {
				// the user should deal exception
				genericParams.put(args[i].toString(),  context.resolve(genericTypes[i]));
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		return Optional.of(new TypeInfoFactoryDescriptor(typeToClass(type), result.f2, genericParams));
	}

	static class TypeInfoFactoryDescriptor extends TypeDescriptionResolver.TypeDescription {

		private final Map<String, Type> genericParams;

		private final TypeInfoFactory<?> typeInfoFactory;

		private final Class<?> clazz;

		public TypeInfoFactoryDescriptor(final Class<?> clazz, final TypeInfoFactory<?> typeInfoFactory, final Map<String, Type> genericParams) {
			this.clazz = clazz;
			this.typeInfoFactory = typeInfoFactory;
			this.genericParams = genericParams;
		}

		public Map<String, Type> getGenericParams() {
			return genericParams;
		}

		public TypeInfoFactory<?> getTypeInfoFactory() {
			return typeInfoFactory;
		}

		public Class<?> getClazz() {
			return clazz;
		}

		public Type getType() {
			return clazz;
		}
	}

	/**
	 * Extract {@link TypeInformation} for the type that has {@link TypeInfo} annotation.
	 * @param type  the type needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or components.
	 * @return the {@link TypeInformation} of the given type or {@link Optional#empty()} if the type does not have the annotation
	 * @throws InvalidTypesException if the factory does not create a valid {@link TypeInformation} or if creating generic type failed
	 */
	public Optional<TypeInformation<?>> extract(final Type type, final TypeInformationExtractor.Context context) {

		if (type instanceof TypeInfoFactoryDescriptor) {

			final TypeInfoFactoryDescriptor typeInfoFactoryDescriptor = (TypeInfoFactoryDescriptor) type;

			final Map<String, Type> genericParams = typeInfoFactoryDescriptor.getGenericParams();
			final TypeInfoFactory<?> typeInfoFactory = typeInfoFactoryDescriptor.getTypeInfoFactory();

			final Map<String, TypeInformation<?>> typeInformationOfGenericParams;

			if (!genericParams.isEmpty()) {
				typeInformationOfGenericParams = new HashMap<>();
				for (String name : genericParams.keySet()) {
					typeInformationOfGenericParams.put(name, context.extract(genericParams.get(name)));
				}
			} else {
				typeInformationOfGenericParams = Collections.emptyMap();
			}
			final TypeInformation<?> createdTypeInfo = typeInfoFactory.createTypeInfo(typeInfoFactoryDescriptor.getClazz(), typeInformationOfGenericParams);
			if (createdTypeInfo == null) {
				throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
			}
			return Optional.of(createdTypeInfo);
		}
		return Optional.empty();
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
