/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark_validator

import com.holdenkarau.spark.testing._

import org.scalatest.{Assertions, BeforeAndAfterEach, FunSuite}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.file.Files;

class ValidationTests extends FunSuite with SharedSparkContext {
  val tempPath = Files.createTempDirectory(null).toString()
  // TODO(holden): factor out a bunch of stuff but lets add a first test as a starting point

  test("null validation test") {
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule]())
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    sc.parallelize(1.to(10)).foreach(acc += _)
    assert(v.validate(1) === true)
  }


  test("sample expected failure") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("taskinfo.0.0resultSerializationTime", Some(100), None))
    )
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    sc.parallelize(1.to(10)).foreach(acc += _)
    assert(v.validate(2) === false)
  }

  test("basic rule, expected success") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(100)))
    )
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    sc.parallelize(1.to(10)).foreach(acc += _)
    assert(v.validate(3) === true)
  }
}
