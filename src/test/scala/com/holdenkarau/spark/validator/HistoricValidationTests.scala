/*
 * Verifies that rules involving history work
 */

package com.holdenkarau.spark.validator

import java.nio.file.Files

import com.holdenkarau.spark.testing._
import org.apache.spark.{Accumulator, SparkContext}
import org.scalatest.FunSuite

class HistoricValidationTests extends FunSuite with SharedSparkContext {
  val tempPath = Files.createTempDirectory(null).toString()

  // A simple job we can use for some sanity checking
  def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString()+"/magic")
  }

  test("simple first run test - populating acc") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](new AvgRule("acc", 0, Some(200), newCounter=true)))
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(1) === true)
  }


  test("sample expected failure - should not be included in historic data") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](new AbsoluteSparkCounterValidationRule("resultSerializationTime", Some(1000), None)))
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    // We run this simple job 2x, but since we expect a failure it shouldn't skew the average
    runSimpleJob(sc, acc)
    runSimpleJob(sc, acc)
    assert(validator.validate(2) === false)
  }

  test("basic historic rule") {
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule](new AvgRule("acc", 0.001, Some(200))))
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate(3) === true)
  }

  test("validate historic new counter") {
    val vc = new ValidationConf(tempPath, "1", true,
      List[ValidationRule](new AvgRule("acc2", 0.001, Some(200), newCounter=true)))
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc2")
    runSimpleJob(sc, acc)
    assert(validator.validate(4) === true)
  }

  test("out of range") {
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule](new AvgRule("acc", 0.001, Some(200))))
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    // Run twice so we get a higher acc value
    runSimpleJob(sc, acc)
    runSimpleJob(sc, acc)
    assert(validator.validate(5) === false)
  }
}
