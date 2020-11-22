package org.apache.flink.api.scala.typeutils

import java.util

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerMatchers, TypeSerializerSchemaCompatibility, TypeSerializerUpgradeTestBase}
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase.TestSpecification
import org.apache.flink.testutils.migration.MigrationVersion
import org.hamcrest.Matcher
import org.hamcrest.Matchers.is
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class UnitSerializerUpgradeTest(spec: TestSpecification[Unit, Unit]) extends TypeSerializerUpgradeTestBase[Unit, Unit](spec) {}

object UnitSerializerUpgradeTest {

  @Parameterized.Parameters(name = "Test Specification = {0}")
  def testSpecifications(): util.Collection[TestSpecification[_, _]] = {
    val testSpecifications =
      new util.ArrayList[TypeSerializerUpgradeTestBase.TestSpecification[_, _]]

    for (migrationVersion <- TypeSerializerUpgradeTestBase.MIGRATION_VERSIONS) {
      testSpecifications.add(
        new TypeSerializerUpgradeTestBase.TestSpecification[Unit, Unit](
          "scala-unit-serializer",
          migrationVersion,
          classOf[UnitSerializerSetup],
          classOf[UnitSerializerVerifier]))
    }

    testSpecifications
  }

  final class UnitSerializerSetup
          extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[Unit] {
    override def createPriorSerializer: TypeSerializer[Unit] = new UnitSerializer();

    override def createTestData: Unit = ()
  }

  final class UnitSerializerVerifier extends
          TypeSerializerUpgradeTestBase.UpgradeVerifier[Unit] {
    override def createUpgradedSerializer: TypeSerializer[Unit] = new UnitSerializer();

    override def testDataMatcher: Matcher[Unit] = is(())

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[Unit]] =
      TypeSerializerMatchers.isCompatibleAsIs[Unit]
  }

}
