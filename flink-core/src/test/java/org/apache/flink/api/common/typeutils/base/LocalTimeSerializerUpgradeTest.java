package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;

/**
 * TODO java doc.
 */
@RunWith(Parameterized.class)
public class LocalTimeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<LocalTime, LocalTime> {

	private static final String SPEC_NAME = "local-time-serializer";

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
					new TestSpecification<>(
							SPEC_NAME,
							migrationVersion,
							LocalTimeSerializerSetup.class,
							LocalTimeSerializerVerifier.class));
		}
		return testSpecifications;
	}

	public LocalTimeSerializerUpgradeTest(TestSpecification<LocalTime, LocalTime> testSpecification) {
		super(testSpecification);
	}

	/**
	 * TODO java doc.
	 */
	public static final class LocalTimeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<LocalTime> {

		@Override
		public TypeSerializer<LocalTime> createPriorSerializer() {
			return LocalTimeSerializer.INSTANCE;
		}

		@Override
		public LocalTime createTestData() {
			return LocalTime.of(10, 33, 40, 100);
		}
	}

	/**
	 * TODO java doc.
	 */
	public static final class LocalTimeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<LocalTime> {

		@Override
		public TypeSerializer<LocalTime> createUpgradedSerializer() {
			return LocalTimeSerializer.INSTANCE;
		}

		@Override
		public Matcher<LocalTime> testDataMatcher() {
			return is(LocalTime.of(10, 33, 40, 100));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<LocalTime>> schemaCompatibilityMatcher(
				MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
