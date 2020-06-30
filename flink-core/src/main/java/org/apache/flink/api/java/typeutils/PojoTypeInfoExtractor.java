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
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getAllDeclaredFields;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

/**
 * This class extracts pojo type information.
 */
public class PojoTypeInfoExtractor extends AutoRegisterDisabledTypeInformationExtractor {

	public static final PojoTypeInfoExtractor INSTANCE = new PojoTypeInfoExtractor();

	private static final Logger LOG = LoggerFactory.getLogger(PojoTypeInfoExtractor.class);

	@Override
	public Optional<Type> resolve(final Type type, final ResolveContext context) {
		if (!isClassType(type)) {
			return Optional.empty();
		}
		final Class<?> clazz = typeToClass(type);

		if (!Modifier.isPublic(clazz.getModifiers())) {
			LOG.info("Class " + clazz.getName() + " is not public so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return Optional.empty();
		}

		final List<Field> fields;
		try {
			fields = getAllDeclaredFields(clazz, false);
		} catch (InvalidTypesException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unable to handle type {} " + clazz + " as POJO. Message: " + e.getMessage(), e);
			}
			return Optional.empty();
		}
		if (fields.size() == 0) {
			LOG.info("No fields were detected for " + clazz + " so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return Optional.empty();
		}

		final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(type, Object.class);

		final List<Tuple2<Field, Type>> pojoFieldTypeDescriptions = new ArrayList<>();
		for (Field field : fields) {
			Type fieldType = field.getGenericType();
			if (!isValidPojoField(field, clazz, typeHierarchy)) {
				LOG.info("Class " + clazz + " cannot be used as a POJO type because not all fields are valid POJO fields, " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return Optional.empty();
			}
			try {
				final Type resolveFieldType = resolveTypeFromTypeHierarchy(fieldType, typeHierarchy, true);
				final Type resolveFieldTypeDescription = context.resolve(resolveFieldType);

				pojoFieldTypeDescriptions.add(Tuple2.of(field, resolveFieldTypeDescription));
			} catch (InvalidTypesException e) {
				// TODO:: This exception handle leads to inconsistent behaviour when Tuple & TypeFactory fail.
				Class<?> genericClass = Object.class;
				if (isClassType(fieldType)) {
					genericClass = typeToClass(fieldType);
				}
				pojoFieldTypeDescriptions.add(Tuple2.of(field, new TypeInformationExtractorFinder.GenericTypeInfoExtractor.GenericTypeDescription(genericClass)));
			}
		}

		//TODO:: move check before the create pojoFieldTypeDescriptions
		//
		// Validate the correctness of the pojo.
		// returning "null" will result create a generic type information.
		//
		List<Method> methods = getAllDeclaredMethods(clazz);
		for (Method method : methods) {
			if (method.getName().equals("readObject") || method.getName().equals("writeObject")) {
				LOG.info("Class " + clazz + " contains custom serialization methods we do not call, so it cannot be used as a POJO type " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return Optional.empty();
			}
		}

		// Try retrieving the default constructor, if it does not have one
		// we cannot use this because the serializer uses it.
		Constructor defaultConstructor = null;
		try {
			defaultConstructor = clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
				LOG.info(clazz + " is abstract or an interface, having a concrete "
					+ "type can increase performance.");
			} else {
				LOG.info(clazz + " is missing a default constructor so it cannot be used as a POJO type "
					+ "and must be processed as GenericType. Please read the Flink documentation "
					+ "on \"Data Types & Serialization\" for details of the effect on performance.");
				return Optional.empty();
			}
		}
		if (defaultConstructor != null && !Modifier.isPublic(defaultConstructor.getModifiers())) {
			LOG.info("The default constructor of " + clazz + " is not Public so it cannot be used as a POJO type "
				+ "and must be processed as GenericType. Please read the Flink documentation "
				+ "on \"Data Types & Serialization\" for details of the effect on performance.");
			return Optional.empty();
		}

		return Optional.of(new PojoTypeDescription(clazz, pojoFieldTypeDescriptions));
	}

	/**
	 * Extract the {@link TypeInformation} for the POJO type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or field components.
	 * @return the {@link TypeInformation} of the given type or {@link Optional#empty()} if the type is not a pojo type
	 */
	public Optional<TypeInformation<?>> extract(final Type type, final TypeInformationExtractor.Context context) {

		if (type instanceof PojoTypeDescription) {
			return Optional.of(((PojoTypeDescription) type).create());
//			final PojoTypeDescription pojoTypeDescription = (PojoTypeDescription) type;
//			List<PojoField> pojoFields = new ArrayList<>();
//
//			for (Tuple2<Field, Type> t : pojoTypeDescription.getFields()) {
//				try {
//					pojoFields.add(new PojoField(t.f0, context.extract(t.f1)));
//				} catch (InvalidTypesException e) {
//					// TODO:: This exception handle leads to inconsistent behaviour when Tuple & TypeFactory fail.
//					Class<?> genericClass = Object.class;
//					if (isClassType(t.f0.getGenericType())) {
//						genericClass = typeToClass(t.f0.getGenericType());
//					}
//					pojoFields.add(new PojoField(t.f0, context.extract(new TypeInformationExtractorFinder.GenericTypeInfoExtractor.GenericTypeDescription(genericClass))));
//				}
//			}
//
//			return Optional.of(new PojoTypeInfo<>(pojoTypeDescription.getClazz(), pojoFields));
		}

		return Optional.empty();
	}

	/**
	 * Checks if the given field is a valid pojo field:
	 * - it is public
	 * OR
	 *  - there are getter and setter methods for the field.
	 *
	 * @param f field to check
	 * @param clazz class of field
	 * @param typeHierarchy type hierarchy for materializing generic types
	 */
	private static boolean isValidPojoField(Field f, Class<?> clazz, List<ParameterizedType> typeHierarchy) {
		if (Modifier.isPublic(f.getModifiers())) {
			return true;
		} else {
			boolean hasGetter = false, hasSetter = false;
			final String fieldNameLow = f.getName().toLowerCase().replaceAll("_", "");

			Type fieldType = f.getGenericType();
			Class<?> fieldTypeWrapper = ClassUtils.primitiveToWrapper(f.getType());

			TypeVariable<?> fieldTypeGeneric = null;
			if (fieldType instanceof TypeVariable) {
				fieldTypeGeneric = (TypeVariable<?>) fieldType;
				fieldType = TypeResolver.resolveTypeFromTypeHierarchy(fieldType, typeHierarchy, true);
			}
			for (Method m : clazz.getMethods()) {
				final String methodNameLow = m.getName().endsWith("_$eq") ?
					m.getName().toLowerCase().replaceAll("_", "").replaceFirst("\\$eq$", "_\\$eq") :
					m.getName().toLowerCase().replaceAll("_", "");

				// check for getter
				if (// The name should be "get<FieldName>" or "<fieldName>" (for scala) or "is<fieldName>" for boolean fields.
					(methodNameLow.equals("get" + fieldNameLow)
						|| methodNameLow.equals("is" + fieldNameLow)
						|| methodNameLow.equals(fieldNameLow))
						&& m.getParameterTypes().length == 0 // no arguments for the getter
						// return type is same as field type (or the generic variant of it)
						&& (m.getGenericReturnType().equals(fieldType)
						|| (m.getReturnType().equals(fieldTypeWrapper))
						|| (fieldTypeGeneric != null && m.getGenericReturnType().equals(fieldTypeGeneric)))) {
					hasGetter = true;
				}
				// check for setters (<FieldName>_$eq for scala)
				if ((methodNameLow.equals("set" + fieldNameLow) || methodNameLow.equals(fieldNameLow + "_$eq"))
					&& m.getParameterTypes().length == 1 // one parameter of the field's type
					&& (m.getGenericParameterTypes()[0].equals(fieldType)
					|| (m.getParameterTypes()[0].equals(fieldTypeWrapper))
					|| (fieldTypeGeneric != null && m.getGenericParameterTypes()[0].equals(fieldTypeGeneric)))
					// return type is void (or the class self).
					&& (m.getReturnType().equals(Void.TYPE) || m.getReturnType().equals(clazz))) {
					hasSetter = true;
				}
			}
			if (hasGetter && hasSetter) {
				return true;
			} else {
				if (!hasGetter) {
					LOG.info(clazz + " does not contain a getter for field " + f.getName());
				}
				if (!hasSetter) {
					LOG.info(clazz + " does not contain a setter for field " + f.getName());
				}
				return false;
			}
		}
	}

	// make this method public only for the avro extraction
	public static Optional<TypeInformation<?>> extract(final Type type) {
		//TODO:: use createType??
		final List<Class<?>> currentExtractingClasses = isClassType(type) ? Collections.singletonList(typeToClass(type)) : Collections.emptyList();

		final Optional<Type> curT =
			PojoTypeInfoExtractor.INSTANCE.resolve(type,
				new TypeDescriptionResolver.TypeDescriptionResolveContext(currentExtractingClasses, Collections.emptyMap()));
		if (curT.isPresent()) {
			//TODO:: maybe we need a not empty current extracting class?? see
			return PojoTypeInfoExtractor.INSTANCE.extract(curT.get(), TypeInfoExtractContext.CONTEXT);
		}
		return Optional.empty();
	}

	class PojoTypeDescription extends TypeDescriptionResolver.TypeDescription {

		private final List<Tuple2<Field, Type>> fields;

		private final Class<?> clazz;

		public PojoTypeDescription(final Class<?> clazz, final List<Tuple2<Field, Type>> fields) {
			this.clazz = clazz;
			this.fields = fields;
		}

		public List<Tuple2<Field, Type>> getFields() {
			return fields;
		}

		public Class<?> getClazz() {
			return clazz;
		}

		@Override
		Type getType() {
			return clazz;
		}

		@Override
		TypeInformation<?> create() {
			List<PojoField> pojoFields = new ArrayList<>();

			for (Tuple2<Field, Type> t : fields) {
				try {
					TypeDescriptionResolver.TypeDescription typeDescription = (TypeDescriptionResolver.TypeDescription) t.f1;
					pojoFields.add(new PojoField(t.f0, typeDescription.create()));
				} catch (InvalidTypesException e) {
					// TODO:: This exception handle leads to inconsistent behaviour when Tuple & TypeFactory fail.
					Class<?> genericClass = Object.class;
					if (isClassType(t.f0.getGenericType())) {
						genericClass = typeToClass(t.f0.getGenericType());
					}
					pojoFields.add(new PojoField(t.f0, new TypeInformationExtractorFinder.GenericTypeInfoExtractor.GenericTypeDescription(genericClass).create()));
				}
			}

			return new PojoTypeInfo<>(clazz, pojoFields);
		}
	}
}
