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
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeInformationExtractorFinder.findTypeInfoExtractor;

public class TypeDescriptionResolver {

	public abstract static class TypeDescription implements Type {
		//TODO:: remove it if we add the "createTypeInformation" method
		abstract Type getType();
	}

	public static class TypeDescriptionResolveContext implements TypeInformationExtractor.ResolveContext {

		private final List<Class<?>> extractingClasses;

		//TODO:: make it immutable
		private final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings;

		public TypeDescriptionResolveContext(final List<Class<?>> extractingClasses,
											 final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings) {
			this.extractingClasses = extractingClasses;
			this.typeVariableBindings = typeVariableBindings;
		}

		@Override
		public List<Class<?>> getExtractingClasses() {
			return extractingClasses;
		}

		@Override
		public Map<TypeVariable<?>, TypeInformation<?>> getTypeVariableBindings() {
			return this.typeVariableBindings;
		}

		@Override
		public Type resolve(final Type type) {
			final List<Class<?>> currentExtractingClasses;
			if (isClassType(type)) {
				currentExtractingClasses =
					ImmutableList.<Class<?>>builder().addAll(extractingClasses).add(typeToClass(type)).build();
			} else {
				currentExtractingClasses = extractingClasses;
			}
			return TypeDescriptionResolver.resolve(
				type,
				new TypeDescriptionResolveContext(currentExtractingClasses, typeVariableBindings));
		}
	}

	static Type resolve(final Type type, final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings) {
		final List<Class<?>> currentExtractingClasses = isClassType(type) ? Collections.singletonList(typeToClass(type)) : Collections.emptyList();
		final Type returnTypeDescription =
			TypeDescriptionResolver.resolve(type, new TypeDescriptionResolver.TypeDescriptionResolveContext(currentExtractingClasses, typeVariableBindings));
		return returnTypeDescription;
	}

	private static Type resolve(final Type type, final TypeInformationExtractor.ResolveContext resolveContext) {
		return findTypeInfoExtractor(type)
			.stream()
			.map(e -> e.resolve(type, resolveContext))
			.filter(Optional::isPresent)
			.map(Optional::get)
			.findFirst()
			.orElseThrow(() -> new InvalidTypesException("Type Information could not be created."));
	}
}
