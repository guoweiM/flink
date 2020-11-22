package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.common.typeutils.{SerializerTestBase, TypeSerializer}
import org.apache.flink.api.scala.typeutils.ScalaTrySerializerUpgradeTest.SpecifiedException

import scala.util.{Failure, Try}

class TrySerializerTest extends SerializerTestBase[Try[String]] {
  override protected def createSerializer(): TypeSerializer[Try[String]] = new TrySerializer[String](StringSerializer.INSTANCE, new ExecutionConfig)

  /**
   * Gets the expected length for the serializer's {@link TypeSerializer#getLength()} method.
   *
   * <p>The expected length should be positive, for fix-length data types, or {@code -1} for
   * variable-length types.
   */
  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[Try[String]] = classOf[Try[String]]

  override protected def getTestData: Array[Try[String]] = Array(Failure(new SpecifiedException("Specified exception for ScalaTry.")))
}
