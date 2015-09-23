/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark_validator

import com.holdenkarau.spark.testing._

import org.scalatest.{Assertions, BeforeAndAfterEach, FunSuite}
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.file.Files;

class ValidationTests extends FunSuite with SharedSparkContext {
  val tempPath = Files.createTempDirectory(null).toString()
  // TODO(holden): factor out a bunch of stuff but lets add a first test as a starting point

  // A simple job we can use for some sanity checking
  def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString()+"/magic")
  }

  test("null validation test") {
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule]())
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(v.validate(1) === true)
  }


  test("sample expected failure") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("resultSerializationTime", Some(1000), None))
    )
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    runSimpleJob(sc, acc)
    assert(v.validate(2) === false)
  }

  test("basic rule, expected success") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(1000)))
    )
    val v = Validation(sc, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(v.validate(3) === true)
  }
}
