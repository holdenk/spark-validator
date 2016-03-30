/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark.validator

import java.nio.file.Files

import com.holdenkarau.spark.testing._
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.sql._
import org.scalatest.FunSuite

/**
 * Test absolute validation rules that don't depend on historical data.
 */
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

  // A simple job we can use for some sanity checking
  def runSimpleJob(sc: SparkContext) {
    val input = sc.parallelize(1.to(10), 5)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

  test("null validation test") {
    val vc = new ValidationConf(tempPath, "job_1", true, List[ValidationRule]())
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(1) === true)
  }

  test("sample expected failure") {
    val vc = new ValidationConf(tempPath, "job_2", true,
      List[ValidationRule](new AbsoluteValueRule("resultSerializationTime", Some(1000), None))
    )
    val validator = Validation(sc, vc)
    runSimpleJob(sc)
    assert(validator.validate(1) === false)
  }

  test("basic rule, expected success") {
    val vc = new ValidationConf(tempPath, "job_3", true,
      List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(1000)))
    )
    val validator = Validation(sc, vc)
    runSimpleJob(sc)
    assert(validator.validate(1) === true)
  }

  test("basic rule, expected success, alt constructor") {
    val vc = new ValidationConf(tempPath, "job_4", true,
      List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)
    runSimpleJob(sc)
    assert(validator.validate(1) === true)
  }

  // Note: this is based on our README so may fail if it gets long or deleted
  test("records read test") {
    val vc = new ValidationConf(tempPath, "job_5", true,
      List[ValidationRule](new AbsoluteValueRule("recordsRead", Some(30), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    import com.google.common.io.Files
    sc.textFile("./README.md").map(_.length).saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
    assert(validator.validate(1) === true)
  }

  // Verify that our listener handles task errors well
  test("random task failure test") {
    val vc = new ValidationConf(tempPath, "job_6", true,
      List[ValidationRule](new AbsoluteValueRule("duration", Some(1), Some(90000)))
    )
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
    assert(validator.validate(1) === true)
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
    //tag::validationExample[]
    val vc = new ValidationConf(tempPath, "job_7", true,
      List[ValidationRule](
        new AbsolutePercentageRule("invalidRecords", "validRecords", Some(0.0), Some(1.0)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    val valid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")

    val invalid = sc.accumulator(0)
    validator.registerAccumulator(invalid, "invalidRecords")

    runTwoCounterJob(sc, valid, invalid)
    //end::validationExample[]
    assert(validator.validate(1) === true)
  }

  test("two counter test, fail") {
    val vc = new ValidationConf(tempPath, "job_8", true,
      List[ValidationRule](
        new AbsolutePercentageRule("invalidRecords", "validRecords", Some(0.0), Some(0.1)))
    )
    val sqlCtx = new SQLContext(sc)
    val validator = Validation(sqlCtx, vc)

    val valid = sc.accumulator(0)
    validator.registerAccumulator(valid, "validRecords")

    val invalid = sc.accumulator(0)
    validator.registerAccumulator(invalid, "invalidRecords")

    runTwoCounterJob(sc, valid, invalid)

    assert(validator.validate(1) === false)
  }

  test("sample expected failure - should not be included in historic data") {
    val vc = new ValidationConf(tempPath, "job_9", true,
      List[ValidationRule](new AbsoluteValueRule("resultSerializationTime", Some(1000), None)))

    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    // We run this simple job 2x, but since we expect a failure it shouldn't skew the average
    runSimpleJob(sc, acc)
    runSimpleJob(sc, acc)

    assert(validator.validate(1) === false)
  }

}
