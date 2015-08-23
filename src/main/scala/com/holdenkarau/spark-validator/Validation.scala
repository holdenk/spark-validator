/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable.HashMap

import org.apache.spark.Accumulator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

class Validation(sc: SparkContext, sqlContext: SQLContext, config: ValidationConf) {
  case class typedAccumulators(
    doubles: HashMap[String, Accumulator[Double]],
    ints: HashMap[String, Accumulator[Int]],
    floats: HashMap[String, Accumulator[Float]],
    longs: HashMap[String, Accumulator[Long]]) {
  }

  protected val accumulators = new typedAccumulators(
    new HashMap[String, Accumulator[Double]](),
    new HashMap[String, Accumulator[Int]](),
    new HashMap[String, Accumulator[Float]](),
    new HashMap[String, Accumulator[Long]]())

  protected val validationListener = new ValidationListener()
  sc.addSparkListener(validationListener)

  /*
   * Register an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name.
   */
  def registerAccumulator(accumulator: Accumulator[Double],
    name: String)(implicit d: DummyImplicit) {
    accumulators.doubles += ((name, accumulator))
  }
  def registerAccumulator(accumulator: Accumulator[Int],
    name: String)(implicit d: DummyImplicit,  d2: DummyImplicit) {
    accumulators.ints += ((name, accumulator))
  }
  def registerAccumulator(accumulator: Accumulator[Long],
    name: String)(implicit d: DummyImplicit,  d2: DummyImplicit,  d3: DummyImplicit) {
    accumulators.longs += ((name, accumulator))
  }
  def registerAccumulator(accumulator: Accumulator[Float],
    name: String) {
    accumulators.floats += ((name, accumulator))
  }

  /*
   * Validates a run of a Spark Job. Returns true if the job is valid and
   * also adds a SUCCESS marker to the path specified.
   * Takes in a jobid, jobids should be monotonically increasing and unique per job.
   */
  def validate(jobid: Long): Boolean = {
    // Make a copy of the validation listener so that if we trigger
    // any work on the spark context this does not update our counters from this point
    // forward.
    val validationListenerCopy = validationListener.copy()
    // Also fetch all the accumulators values
    val oldRuns = findOldCounters()
    // Format the current data
    val currentData = makeHistoricData(jobid, accumulators, validationListenerCopy)
    // Run through all of our rules until one fails
    val failedRuleOption = config.rules.find(rule => ! rule.validate(oldRuns, currentData))
    // if we failed return false otherwise return true
    failedRuleOption.map(_ => false).getOrElse(true)
  }
  /*
   * Convert both Spark counters & user counters into a HistoricData object
   */
  private def makeHistoricData(id: Long, accumulators: typedAccumulators, vl: ValidationListener):
      HistoricData = {
    val counters = accumulators.doubles.map{case (key, value) =>
      (key, value.value.toLong) // We loose info, but w/e
    } ++
    accumulators.longs.map{case (key, value) =>
      (key, value.value) // We loose info, but w/e
    } ++
    accumulators.ints.map{case (key, value) =>
      (key, value.value.toLong) // We loose info, but w/e
    } ++ vl.toMap().map{case (key, value) =>
        (key, value)
    }
    HistoricData(id, counters.toMap)
  }
  private def saveCounters(historicData: HistoricData, success: Boolean) = {
    val prefix = success match {
      case true => "SUCCESS"
      case false => "FAILURE"
    }
    import sqlContext.implicits._
    val id = historicData.jobid
    val data = sqlContext.createDataFrame(historicData.counters.toList)
    val path = config.jobBasePath + "/" + config.jobDir + "/validator/HistoricDataParquet/status=" + prefix + "/id="+id+"/"
    data.write.format("parquet").save(path)
  }

  private def findOldCounters(): List[HistoricData] = {
    List[HistoricData]()
  }
}

object Validation {
  def apply(sc: SparkContext, sqlContext: SQLContext, config: ValidationConf): Validation = {
    new Validation(sc, sqlContext, config)
  }
  def apply(sc: SparkContext, config: ValidationConf): Validation = {
    new Validation(sc, new SQLContext(sc), config)
  }
}
