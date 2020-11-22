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

package org.apache.flink.test.completeness;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Scans the class path for type serializer and checks if there is a test for it.
 */
public class TypeSerializerTestCoverageTest extends TestLogger {

	/** Following already covered by the {@link org.apache.flink.api.common.typeutils.base.BasicTypeSerializerUpgradeTest}. */
	static final Set<String> ALREADY_COVERED_SET1 = new HashSet<>(Arrays.asList(
			"org.apache.flink.api.common.typeutils.base.CharValueSerializer",
			"org.apache.flink.api.common.typeutils.base.DateSerializer",
			"org.apache.flink.api.common.typeutils.base.CharSerializer"));

	static final Set<String> NEED_TO_FIX = new HashSet<>(Arrays.asList("org.apache.flink.api.scala.typeutils.UnitSerializer"));

	static final Set<String> WHITE_NAME_LIST_OF_UPGRADE_TEST = new HashSet<>(ALREADY_COVERED_SET1);

	static final Set<String> WHITE_NAME_LIST_OF_SERIALIZER_TEST = new HashSet<>(NEED_TO_FIX);

	@Test
	public void testTypeSerializerTestCoverage() {
		final Reflections reflections = new Reflections("org.apache.flink");

		final Set<Class<? extends TypeSerializer>> typeSerializers = reflections.getSubTypesOf(
				TypeSerializer.class);

		final Set<String> typeSerializerTestNames = reflections
				.getSubTypesOf(SerializerTestBase.class)
				.stream()
				.map(Class::getName)
				.collect(Collectors.toSet());
		final Set<String> typeSerializerUpgradeTestNames = reflections
				.getSubTypesOf(TypeSerializerUpgradeTestBase.class)
				.stream()
				.map(Class::getName)
				.collect(Collectors.toSet());

		// check if a test exists for each type serializer
		for (Class<? extends TypeSerializer> typeSerializer : typeSerializers) {
			// we skip abstract classes and inner classes to skip type serializer defined in test classes
			if (Modifier.isAbstract(typeSerializer.getModifiers()) ||
					Modifier.isPrivate(typeSerializer.getModifiers()) ||
					typeSerializer.getName().contains("Test") ||
					typeSerializer.getName().contains("ITCase") ||
					typeSerializer.getName().contains("$") ||
					typeSerializer.getName().contains("testutils") ||
//					typeSerializer.getName().contains("typeutils") ||
					typeSerializer.getName().contains("state") ||
					typeSerializer.getName().contains("TypeSerializerProxy") ||
					typeSerializer.getName().contains("KeyAndValueSerializer")) {
				continue;
			}

			final String testToFind = typeSerializer.getName() + "Test";
			if (!typeSerializerTestNames.contains(testToFind) && !WHITE_NAME_LIST_OF_SERIALIZER_TEST
					.contains(typeSerializer.getName())) {
				fail("Could not find test '" + testToFind + "' that covers '"
						+ typeSerializer.getName() + "'.");
			}

			final String upgradeTestToFind = typeSerializer.getName() + "UpgradeTest";
			if (!typeSerializerUpgradeTestNames.contains(upgradeTestToFind)
					&& !WHITE_NAME_LIST_OF_UPGRADE_TEST
					.contains(typeSerializer.getName())) {
				fail("Could not find upgrade test '" + upgradeTestToFind + "' that covers '"
						+ typeSerializer.getName() + "'.");
			}
		}
	}
}
