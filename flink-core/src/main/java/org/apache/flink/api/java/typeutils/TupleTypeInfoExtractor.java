package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.countFieldsInClass;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This extractor is used to extract the {@link TypeInformation} for the tuple.
 * @param <T> the type for which the extractor creates.
 */
public class TupleTypeInfoExtractor<T extends Tuple> implements TypeInformationExtractor<T> {

	public TupleTypeInfoExtractor() {

	}

	@Override
	public List<Type> getTypeHandled() {
		return Arrays.asList(Tuple.class);
	}

	@Override
	public TypeInformation<? extends T> create(Type type, Context context) {

		final List<Type> typeHierarchy = new ArrayList<Type>(Arrays.asList(type));

		Type curT = type;

		// do not allow usage of Tuple as type
		if (typeToClass(type).equals(Tuple.class)) {
			throw new InvalidTypesException(
				"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
		}

		// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
		// collect the types while moving up for a later top-down
		while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
			typeHierarchy.add(curT);
			curT = typeToClass(curT).getGenericSuperclass();
		}

		if (curT == Tuple0.class) {
			return new TupleTypeInfo(Tuple0.class);
		}

		// check if immediate child of Tuple has generics
		if (curT instanceof Class<?>) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		typeHierarchy.add(curT);

		curT = TypeExtractionUtils.resolveTypeFromTypeHierachy(curT, typeHierarchy, true);

		// create the type information for the subtypes
		final TypeInformation<?>[] subTypesInfo = createSubTypesInfo(type, (ParameterizedType) curT, context);

		if (subTypesInfo == null) {
			return null;
		}

		// return tuple info
		return new TupleTypeInfo(typeToClass(type), subTypesInfo);
	}

	private TypeInformation<?>[] createSubTypesInfo(final Type originalType,
													final ParameterizedType definingType,
													final Context context) {

		final int typeArgumentsLength = definingType.getActualTypeArguments().length;
		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];
		for (int i = 0; i < typeArgumentsLength; i++) {
			final Type actualTypeArgument = definingType.getActualTypeArguments()[i];
			subTypesInfo[i] = context.create(actualTypeArgument);

			if (subTypesInfo[i] == null && actualTypeArgument instanceof TypeVariable) {
				throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) actualTypeArgument).getName() + "' in '"
					+ ((TypeVariable<?>) actualTypeArgument).getGenericDeclaration()
					+ "' could not be determined. This is most likely a type erasure problem. "
					+ "The type extraction currently supports types with generic variables only in cases where "
					+ "all variables in the return type can be deduced from the input type(s). "
					+ "Otherwise the type has to be specified explicitly using type information.");
			}
		}

		Class<?> originalTypeAsClass = typeToClass(originalType);

		// check for additional fields.
		int fieldCount = countFieldsInClass(originalTypeAsClass);
		if (fieldCount > subTypesInfo.length) {
				return null;
		}

		return subTypesInfo;
	}
}
