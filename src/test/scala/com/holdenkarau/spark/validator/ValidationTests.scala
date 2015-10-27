/*
 * Verifies that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark.validator

import com.holdenkarau.spark.testing._

import org.scalatest.{Assertions, BeforeAndAfterEach, FunSuite}
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
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

  test("basic rule, expected success, alt constructor") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val v = Validation(sc, sqlCtx, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(v.validate(4) === true)
  }

  // Note: this is based on our README so may fail if it gets long or deleted
  test("records read test") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("recordsRead", Some(30), Some(1000)))
    )
    val sqlCtx = new SQLContext(sc)
    val v = Validation(sc, sqlCtx, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    import com.google.common.io.Files
    sc.textFile("./README.md").map(_.length).saveAsTextFile(Files.createTempDir().toURI().toString()+"/magic")
    assert(v.validate(5) === true)
  }

  // Verify that our listener handles task errors well
  test("random task failure test") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](
        new AbsoluteSparkCounterValidationRule("duration", Some(1), Some(20000)))
    )
    val sqlCtx = new SQLContext(sc)
    val v = Validation(sc, sqlCtx, vc)
    val acc = sc.accumulator(0)
    v.registerAccumulator(acc, "acc")
    val input = sc.parallelize(1.to(200), 100)
    input.map({x =>
      val rand = new scala.util.Random()
      if (rand.nextInt(10) == 1) {
        throw new Exception("fake error")
      }
      x
    })
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString()+"/magic")
    assert(v.validate(6) === true)
  }
}
