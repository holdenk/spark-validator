/*
 * Verifies that rules involving history work
 */

package com.holdenkarau.spark.validator

import java.nio.file.Files
import java.time.LocalDateTime

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

  test("test AverageRuleSameWeekDay") {
    val validationRules1 = List[ValidationRule](new AverageRuleSameWeekDay("acc", 0.001, Some(200), true))
    val vc1 = new ValidationConf(tempPath, "weekJob", true, validationRules1)
    val validator1 = Validation(sc, vc1)
    validator1.setCurrentDate(LocalDateTime.now().minusWeeks(1)) // run as old run, from 1 week
    val acc1 = sc.accumulator(0)
    validator1.registerAccumulator(acc1, "acc")
    sc.parallelize(List(1, 2, 3)).foreach(x => acc1 += x) // acc1 =  6
    assert(validator1.validate() === true)

    val validationRules2 = List[ValidationRule](new AverageRuleSameWeekDay("acc", 0.001, Some(200), false))
    val vc2 = new ValidationConf(tempPath, "weekJob", false, validationRules2)
    val validator2 = Validation(sc, vc2)
    validator2.setCurrentDate(LocalDateTime.now().minusDays(3)) // run as old run, from 3 days
    val acc2 = sc.accumulator(0)
    validator2.registerAccumulator(acc2, "acc")
    sc.parallelize(List(1, 2, 3, 9)).foreach(x => acc2 += x) // acc = 15
    assert(validator2.validate() === true)

    val validationRules3 = List[ValidationRule](new AverageRuleSameWeekDay("acc", 0.001, Some(200), false))
    val vc3 = new ValidationConf(tempPath, "weekJob", false, validationRules3)
    val validator3 = Validation(sc, vc3)
    validator3.setCurrentDate(LocalDateTime.now()) // run as new run
    val acc3 = sc.accumulator(0)
    validator3.registerAccumulator(acc3, "acc")
    sc.parallelize(List(1, 2, 3)).foreach(x => acc3 += x) // acc = 6
    assert(validator3.validate() === true)
  }

  test("test AverageRuleSameMonthDay") {
    val validationRules1 = List[ValidationRule](new AverageRuleSameMonthDay("acc", 0.001, Some(200), true))
    val vc1 = new ValidationConf(tempPath, "MonthJob", true, validationRules1)
    val validator1 = Validation(sc, vc1)
    validator1.setCurrentDate(LocalDateTime.now().minusMonths(1)) // run as old run, from 1 month
    val acc1 = sc.accumulator(0)
    validator1.registerAccumulator(acc1, "acc")
    sc.parallelize(List(1, 2, 3)).foreach(x => acc1 += x) // acc1 =  6
    assert(validator1.validate() === true)

    val validationRules2 = List[ValidationRule](new AverageRuleSameMonthDay("acc", 0.001, Some(200), false))
    val vc2 = new ValidationConf(tempPath, "MonthJob", false, validationRules2)
    val validator2 = Validation(sc, vc2)
    validator2.setCurrentDate(LocalDateTime.now().minusWeeks(2)) // run as old run, from 3 days
    val acc2 = sc.accumulator(0)
    validator2.registerAccumulator(acc2, "acc")
    sc.parallelize(List(1, 2, 3, 9)).foreach(x => acc2 += x) // acc = 15
    assert(validator2.validate() === true)

    val validationRules3 = List[ValidationRule](new AverageRuleSameMonthDay("acc", 0.001, Some(200), false))
    val vc3 = new ValidationConf(tempPath, "MonthJob", false, validationRules3)
    val validator3 = Validation(sc, vc3)
    validator3.setCurrentDate(LocalDateTime.now()) // run as new run
    val acc3 = sc.accumulator(0)
    validator3.registerAccumulator(acc3, "acc")
    sc.parallelize(List(1, 2, 3)).foreach(x => acc3 += x) // acc = 6
    assert(validator3.validate() === true)
  }

  // A simple job we can use for some sanity checking
  private def runSimpleJob(sc: SparkContext, acc: Accumulator[Int]) {
    val input = sc.parallelize(1.to(10), 5)
    input.foreach(acc += _)
    import com.google.common.io.Files
    input.saveAsTextFile(Files.createTempDir().toURI().toString() + "/magic")
  }

}
