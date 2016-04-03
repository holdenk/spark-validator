/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark.validator

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Validation class that will be used to validate counters data.
 *
 * @param sqlContext
 * @param config validation configurations.
 */
class Validation(sqlContext: SQLContext, config: ValidationConf) {

  protected val accumulators = new typedAccumulators(
    new HashMap[String, Accumulator[Double]](),
    new HashMap[String, Accumulator[Int]](),
    new HashMap[String, Accumulator[Float]](),
    new HashMap[String, Accumulator[Long]]())

  protected val validationListener = new ValidationListener()
  sqlContext.sparkContext.addSparkListener(validationListener)

  private var failedRules: List[(ValidationRule, String)] = _

  private val jobStartDate = getCurrentDate()

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
    val validationListenerCopy = validationListener.copy()

    // Also fetch all the accumulators values
    val oldRuns = findOldHistoricData()
    // Format the current data
    val currentData = HistoricData(accumulators, validationListenerCopy)

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
    saveCounters(currentData, result)
    result
  }

  /**
   * Saves historic data to the path given in validationConf.
   *
   * @param success success flag, true if job validation succeeded, false if validation failed.
   */
  private def saveCounters(historicData: HistoricData, success: Boolean): Unit = {
    // creates accumulator DataFrame
    val schema = StructType(List(
      StructField("counterName", StringType, false),
      StructField("value", LongType, false)))
    val rows = sqlContext.sparkContext.parallelize(historicData.counters.toList).map(kv => Row(kv._1, kv._2))
    val data = sqlContext.createDataFrame(rows, schema)

    // save accumulators DataFrame
    val path = HistoricData.getWritePath(config.jobBasePath, config.jobName, success, jobStartDate.toString)
    data.write.parquet(path)
  }

  /**
   * Gets the Historic Data as an Array.
   */
  private def findOldHistoricData(): Array[HistoricData] = {
    val countersDF = loadOldCounters()
    countersDF match {
      case Some(df) => {
        val historicDataRDD = df.select("date", "counterName", "value").rdd
          .map(row => (Timestamp.valueOf(row.getString(0)), (row.getString(1), row.getLong(2))))
          .groupByKey()
          .map { case (date, counters) => HistoricData(counters.toMap) }

        historicDataRDD.collect()
      }
      case None => {
        new Array[HistoricData](0)
      }
    }
  }

  /**
   * Returns a DataFrame of the old counters (for SQL funtimes).
   */
  private def loadOldCounters(): Option[DataFrame] = {
    val path = HistoricData.getReadPath(config.jobBasePath, config.jobName, true)
    // Spark SQL doesn't handle empty directories very well...
    val fs = org.apache.hadoop.fs.FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      val inputDF = sqlContext.read.parquet(path)
      Some(inputDF)
    } else {
      None
    }
  }

  def getCurrentDate(): Timestamp = {
    new Timestamp(Calendar.getInstance().getTime().getTime)
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

case class typedAccumulators(
  doubles: HashMap[String, Accumulator[Double]],
  ints: HashMap[String, Accumulator[Int]],
  floats: HashMap[String, Accumulator[Float]],
  longs: HashMap[String, Accumulator[Long]]) {

  def toMap(): Map[String, Long] = {
    val accumulatorsMap: mutable.Map[String, Long] =
      doubles.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      } ++
      floats.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      } ++
      longs.map { case (key, value) =>
        (key, value.value)
      } ++
      ints.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      }

    accumulatorsMap.toMap
  }
}

