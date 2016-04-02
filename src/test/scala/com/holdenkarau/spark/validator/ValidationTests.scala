/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark.validator

import java.nio.file.Files

import com.holdenkarau.spark.testing._
import org.apache.spark.sql._
import org.apache.spark.{Accumulator, SparkContext}
import org.scalatest.FunSuite

/**
 * Test absolute validation rules that don't depend on historical data.
 */
class ValidationTests extends FunSuite with SharedSparkContext {
  val tempPath = Files.createTempDirectory(null).toString()

  // TODO(holden): factor out a bunch of stuff but lets add a first test as a starting point

  test("null validation test") {
    val validationRules = List[ValidationRule]()
    val vc = new ValidationConf(tempPath, "job_1", true, validationRules)
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate() === true)
  }

  test("sample expected failure") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("resultSerializationTime", Some(1000), None))
    val vc = new ValidationConf(tempPath, "job_2", true, validationRules)
    val validator = Validation(sc, vc)
    runSimpleJob(sc)
    assert(validator.validate() === false)
  }

  test("basic rule, expected success") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(1000)))
    val vc = new ValidationConf(tempPath, "job_3", true, validationRules)
    val validator = Validation(sc, vc)
    runSimpleJob(sc)
    assert(validator.validate() === true)
  }

  test("basic rule, expected success, alt constructor") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(1000)))
    val vc = new ValidationConf(tempPath, "job_4", true, validationRules)
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    runSimpleJob(sc)
    assert(validator.validate() === true)
  }

  // Note: this is based on our README so may fail if it gets long or deleted
  test("records read test") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("recordsRead", Some(30), Some(1000)))
    val vc = new ValidationConf(tempPath, "job_5", true, validationRules)
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    import com.google.common.io.Files
    sc.textFile("./README.md").map(_.length).saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
    assert(validator.validate() === true)
  }

  // Verify that our listener handles task errors well
  test("random task failure test") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(90000)))
    val vc = new ValidationConf(tempPath, "job_6", true, validationRules)
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val input = sc.parallelize(1 to 200, 100)
    input.map({ x =>
      val rand = new scala.util.Random()
      if (rand.nextInt(10) == 1) {
        throw new Exception("fake error")
      }
      x
    })
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
    assert(validator.validate() === true)
  }

  // Note: this is based on our README so may fail if it gets long or deleted
  test("two counter test, pass") {
    //tag::validationExample[]
    val validationRules = List[ValidationRule](
      new AbsolutePercentageRule("invalidRecords", "validRecords", Some(0.0), Some(1.0)))
    val vc = new ValidationConf(tempPath, "job_7", true, validationRules)
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    val valid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")

    val invalid = sc.accumulator(0)
    validator.registerAccumulator(invalid, "invalidRecords")

    runTwoCounterJob(sc, valid, invalid)
    //end::validationExample[]
    assert(validator.validate() === true)
  }

  test("two counter test, fail") {
    val validationRules = List[ValidationRule](
      new AbsolutePercentageRule("invalidRecords", "validRecords", Some(0.0), Some(0.1)))
    val vc = new ValidationConf(tempPath, "job_8", true, validationRules)
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    val valid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")

    val invalid = sc.accumulator(0)
    validator.registerAccumulator(invalid, "invalidRecords")

    runTwoCounterJob(sc, valid, invalid)

    assert(validator.validate() === false)
  }

  test("sample expected failure - should not be included in historic data") {
    val validationRules = List[ValidationRule](new AbsoluteValueRule("resultSerializationTime", Some(1000), None))
    val vc = new ValidationConf(tempPath, "job_9", true, validationRules)

    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    // We run this simple job 2x, but since we expect a failure it shouldn't skew the average
    runSimpleJob(sc, acc)
    runSimpleJob(sc, acc)

    assert(validator.validate() === false)
  }

  test("test getting failed rules") {
    val rule = new AbsoluteValueRule("resultSerializationTime", Some(1000), None)
    val validationRules = List[ValidationRule](rule)
    val vc = new ValidationConf(tempPath, "job_10", true, validationRules)
    val validator = Validation(sc, vc)
    runSimpleJob(sc)
    assert(validator.validate() === false)

    assert(validator.getFailedRules()(0)._1 == rule)
  }

  // A simple job we can use for some sanity checking
  private def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

  // A simple job we can use for some sanity checking
  private def runSimpleJob(sc: SparkContext) {
    val input = sc.parallelize(1.to(10), 5)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

  // A slightly more complex job
  private def runTwoCounterJob(sc: SparkContext, valid: Accumulator[Int], invalid: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    // Fake rejecting some records
    input.foreach { x =>
      if (x % 5 == 0) {
        invalid += 1
      } else {
        valid += 1
      }
    }
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

}
