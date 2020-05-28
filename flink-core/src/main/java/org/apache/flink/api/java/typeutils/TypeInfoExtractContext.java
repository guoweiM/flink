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

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

class TypeInfoExtractContext implements TypeInformationExtractor.Context {

	public static final TypeInfoExtractContext CONTEXT = new TypeInfoExtractContext(Collections.emptyMap(), Collections.emptyList());

	private final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings;

	private final List<Class<?>> extractingClasses;

	TypeInfoExtractContext(final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings, final List<Class<?>> extractingClasses) {
		this.typeVariableBindings = ImmutableMap.<TypeVariable<?>, TypeInformation<?>>builder().putAll(typeVariableBindings).build();
		this.extractingClasses = ImmutableList.<Class<?>>builder().addAll(extractingClasses).build();
	}

	@Override
	public TypeInformation<?> extract(final Type type) {
		final List<Class<?>> currentExtractingClasses;

		if (isClassType(type)) {
			currentExtractingClasses = new ArrayList<>(extractingClasses);
			currentExtractingClasses.add(typeToClass(type));
		} else {
			currentExtractingClasses = extractingClasses;
		}
		final TypeInfoExtractContext context = new TypeInfoExtractContext(typeVariableBindings, currentExtractingClasses);
		return TypeExtractionUtils.extract(type, context);
	}

	@Override
	public Map<TypeVariable<?>, TypeInformation<?>> getTypeVariableBindings() {
		return typeVariableBindings;
	}

	@Override
	public List<Class<?>> getExtractingClasses() {
		return extractingClasses;
	}
}
