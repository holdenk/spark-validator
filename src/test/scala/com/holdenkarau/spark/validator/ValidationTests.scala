/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark.validator

import java.nio.file.Files

import com.holdenkarau.spark.testing._
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.sql._
import org.scalatest.FunSuite

class ValidationTests extends FunSuite with SharedSparkContext {
  val tempPath = Files.createTempDirectory(null).toString()

  // TODO(holden): factor out a bunch of stuff but lets add a first test as a starting point

  // A simple job we can use for some sanity checking
  def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

  test("null validation test") {
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule]())
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(1) === true)
  }

  test("sample expected failure") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("resultSerializationTime", Some(1000), None))
    )
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    runSimpleJob(sc, acc)
    assert(validator.validate(2) === false)
  }

  test("basic rule, expected success") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(1000)))
    )
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(3) === true)
  }

  test("basic rule, expected success, alt constructor") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(4) === true)
  }

  // Note: this is based on our README so may fail if it gets long or deleted
  test("records read test") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("recordsRead", Some(30), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    import com.google.common.io.Files
    sc.textFile("./README.md").map(_.length).saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
    assert(validator.validate(5) === true)
  }

  // Verify that our listener handles task errors well
  test("random task failure test") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(90000)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    val input = sc.parallelize(1.to(200), 100)
    input.map({ x =>
      val rand = new scala.util.Random()
      if (rand.nextInt(10) == 1) {
        throw new Exception("fake error")
      }
      x
    })
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
    assert(validator.validate(6) === true)
  }

  // A slightly more complex job
  def runTwoCounterJob(sc: SparkContext, valid: Accumulator[Int], invalid: Accumulator[Int]) {
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

  // Note: this is based on our README so may fail if it gets long or deleted
  test("two counter test, pass") {
    val jobid = 7
    //tag::validationExample[]
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsolutePercentageSparkCounterValidationRule(
          "invalidRecords", "validRecords", Some(0.0), Some(1.0)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val valid = sc.accumulator(0)
    val invalid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")
    validator.registerAccumulator(invalid, "invalidRecords")
    runTwoCounterJob(sc, valid, invalid)
    val isValid = validator.validate(jobid)
    //end::validationExample[]
    assert(isValid === true)
  }

  test("two counter test, fail") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsolutePercentageSparkCounterValidationRule(
          "invalidRecords", "validRecords", Some(0.0), Some(0.1)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    val valid = sc.accumulator(0)
    val invalid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")
    validator.registerAccumulator(invalid, "invalidRecords")
    runTwoCounterJob(sc, valid, invalid)
    assert(validator.validate(8) === false)
  }
}
