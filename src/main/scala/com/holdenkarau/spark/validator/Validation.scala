/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark.validator

import java.time.LocalDateTime

import org.apache.spark.sql._
import org.apache.spark.{Accumulator, SparkContext, ValidatorSparkContext}

import scala.collection.mutable.HashMap

/**
 * Validation class that will be used to validate counters data.
 *
 * @param sqlContext
 * @param config validation configurations.
 */
class Validation(sqlContext: SQLContext, config: ValidationConf) {

  protected val accumulators = new TypedAccumulators(
    new HashMap[String, Accumulator[Double]](),
    new HashMap[String, Accumulator[Int]](),
    new HashMap[String, Accumulator[Float]](),
    new HashMap[String, Accumulator[Long]]())

  protected val validationListener = new ValidationListener()
  sqlContext.sparkContext.addSparkListener(validationListener)

  private var failedRules: List[(ValidationRule, String)] = _

  private var jobStartDate = getCurrentDate()

  /**
   * Registers an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name. Should register accumulators before validating.
   */
  def registerAccumulator(accumulator: Accumulator[Double], accumulatorName: String)
                         (implicit d: DummyImplicit) {
    accumulators.doubles += ((accumulatorName, accumulator))
  }

  /**
   * Registers an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name. Should register accumulators before validating.
   */
  def registerAccumulator(accumulator: Accumulator[Int], accumulatorName: String)
                         (implicit d: DummyImplicit, d2: DummyImplicit) {
    accumulators.ints += ((accumulatorName, accumulator))
  }

  /**
   * Registers an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name. Should register accumulators before validating.
   */
  def registerAccumulator(accumulator: Accumulator[Long], accumulatorName: String)
                         (implicit d: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit) {
    accumulators.longs += ((accumulatorName, accumulator))
  }

  /**
   * Registers an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name. Should register accumulators before validating.
   */
  def registerAccumulator(accumulator: Accumulator[Float], accumulatorName: String) {
    accumulators.floats += ((accumulatorName, accumulator))
  }

  /**
   * Gets failed rules after validation. Returns list of (failed validation rule, failure message).
   * To get the failed rules you must validate first.
   */
  def getFailedRules(): List[(ValidationRule, String)] = failedRules

  /**
   * Validates a run of a Spark Job. Returns true if the job is valid and
   * also adds a SUCCESS marker to the path specified. Takes in a jobid.
   * jobids should be monotonically increasing and unique per job.
   *
   * It is recommended to call validate method only once per object. If you want to
   * validate another accumulator create another Validation object.
   *
   * @return Returns true if the job is valid, false if not valid.
   */
  def validate(): Boolean = {
    // Make a copy of the validation listener so that if we trigger
    // any work on the spark context this does not update our counters from this point
    // forward.
    ValidatorSparkContext.waitUntilEmpty(sqlContext)
    val validationListenerCopy = validationListener.copy()

    // Also fetch all the accumulators values
    val historicDataPath = HistoricData.getPath(config.jobBasePath, config.jobName, true)
    val oldRuns: Array[HistoricData] = HistoricData.loadHistoricData(sqlContext, historicDataPath)

    // Format the current data
    val currentData = HistoricData(accumulators, validationListenerCopy, jobStartDate)
    // Run through all of our rules until one fails
    failedRules =
      config.rules.flatMap(rule => {
        val validationResult = rule.validate(oldRuns, currentData)
        if (validationResult.isDefined) {
          Some(rule, validationResult.get)
        } else
          None
      })

    if (failedRules.nonEmpty) {
      println("Failed rules include " + failedRules)
    }

    // if we failed return false otherwise return true
    val result = failedRules.isEmpty

    val currentDataPath = HistoricData.getPath(config.jobBasePath, config.jobName, result)
    currentData.saveHistoricData(sqlContext, currentDataPath)

    result
  }

  def getCurrentDate(): LocalDateTime = {
    LocalDateTime.now
  }

  /**
   * For testing purposes only.
   */
  private[validator] def setCurrentDate(time: LocalDateTime) {
    jobStartDate = time
  }
}

object Validation {
  /**
   * Creates a Validation class, that will be used to validate counters data.
   * Important Note: validation class should be created before running the job,
   * So it can store Spark's built in metrics (bytes read, time, etc.)
   *
   * @param sqlContext
   * @param config Validation configurations.
   */
  def apply(sqlContext: SQLContext, config: ValidationConf): Validation = {
    new Validation(sqlContext, config)
  }

  /**
   * Creates a Validation class, that will be used to validate counters data.
   * Important Note: validation class should be created before running the job,
   * So it can store Spark's built in metrics (bytes read, time, etc.)
   *
   * @param sc     Spark Context.
   * @param config Validation configurations.
   */
  def apply(sc: SparkContext, config: ValidationConf): Validation = {
    new Validation(new SQLContext(sc), config)
  }
}