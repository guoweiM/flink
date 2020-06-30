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

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Optional;

class TypeVariableExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	static final TypeVariableExtractor INSTANCE = new TypeVariableExtractor();
	@Override
	public Optional<TypeDescription> resolve(final Type type, final Context context) {
		if (type instanceof TypeVariable) {
			final TypeVariable typeVariable = (TypeVariable) type;
			final TypeInformation typeInformation = context.getTypeVariableBindings().get(typeVariable);
			if (typeInformation == null) {
				throw new InvalidTypesException("Type of TypeVariable '" + typeVariable.getName() + "' in '"
					+ typeVariable.getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
					+ "The type extraction currently supports types with generic variables only in cases where "
					+ "all variables in the return type can be deduced from the input type(s). "
					+ "Otherwise the type has to be specified explicitly using type information.");
			}
			return Optional.of(new TypeVariableDescription(context.getTypeVariableBindings().get(typeVariable)));
		}
		return Optional.empty();
	}

	static class TypeVariableDescription implements TypeDescription {

		private final TypeInformation<?> typeInformationOfTypeVariable;

		public TypeVariableDescription(final TypeInformation<?> typeInformationOfTypeVariable) {
			this.typeInformationOfTypeVariable = typeInformationOfTypeVariable;
		}

		@Override
		public TypeInformation<?> create() {
			return typeInformationOfTypeVariable;
		}
	}
}
