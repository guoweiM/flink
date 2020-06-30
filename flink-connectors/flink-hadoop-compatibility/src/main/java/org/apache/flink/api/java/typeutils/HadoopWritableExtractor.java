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
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.io.Writable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is used to resolve the {@link TypeDescription} for Hadoop writable.
 */
public class HadoopWritableExtractor extends TypeInformationExtractorForClass {

	@Override
	public Optional<TypeDescription> resolve(final Class<?> clazz) {
		if (TypeExtractionUtils.isHadoopWritable(typeToClass(clazz))) {
			return Optional.of(new HadoopWritableTypeDescription(clazz));
		}
		return Optional.empty();
	}

	@Override
	public List<Class<?>> getClasses() {
		return Collections.singletonList(Writable.class);
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	static <T> TypeInformation<T> createHadoopWritableTypeInfo(Class<T> clazz) {
		//TODO:: refactory this
		checkNotNull(clazz);
		return new WritableTypeInfo(clazz);
	}

	class HadoopWritableTypeDescription extends ClassDescription {

		public HadoopWritableTypeDescription(final Class<?> clazz) {
			super(clazz);
		}

		@Override
		public TypeInformation<?> create() {
			return createHadoopWritableTypeInfo(this.getClazz());
		}
	}
}
