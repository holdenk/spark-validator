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

  test("simple first run test - populating acc") {
    val validationRules = List[ValidationRule](new AverageRule("acc", 0, Some(200), newCounter = true))
    val vc = new ValidationConf(tempPath, "1", true, validationRules)
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate() === true)
  }

  test("basic historic rule") {
    val validationRules = List[ValidationRule](new AverageRule("acc", 0.001, Some(200)))
    val vc = new ValidationConf(tempPath, "1", true, validationRules)
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    runSimpleJob(sc, acc)
    assert(validator.validate() === true)
  }

  test("validate historic new counter") {
    val validationRules = List[ValidationRule](new AverageRule("acc2", 0.001, Some(200), newCounter = true))
    val vc = new ValidationConf(tempPath, "1", true, validationRules)
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc2")
    runSimpleJob(sc, acc)
    assert(validator.validate() === true)
  }

  test("out of range") {
    val validationRules = List[ValidationRule](new AverageRule("acc", 0.001, Some(200)))
    val vc = new ValidationConf(tempPath, "1", true, validationRules)
    val validator = Validation(sc, vc)
    val acc = sc.accumulator(0)
    validator.registerAccumulator(acc, "acc")
    // Run twice so we get a higher acc value
    runSimpleJob(sc, acc)
    runSimpleJob(sc, acc)
    assert(validator.validate() === false)
  }

  // A simple job we can use for some sanity checking
  private def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

}
