/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Accumulator

import com.holdenkarau.spark_validator.HistoricDataProtos.CounterInfo
import com.holdenkarau.spark_validator.HistoricDataProtos.HistoricData

class Validation(sc: SparkContext, config: ValidationConf) {
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
   */
  def validate(): Boolean = {
    // Make a copy of the validation listener so that if we trigger
    // an work on the spark context this does not update our counters from this point
    // forward.
    val validationListenerCopy = validationListener.copy()
    // Also fetch all the accumulators values
    val oldRuns = findOldCounters()
    // Format the current data
    val currentData = makeHistoricData(accumulators, validationListenerCopy)
    // Run through all of our rules until one fails
    val failedRuleOption = config.rules.find(rule => ! rule.validate(oldRuns, currentData))
    // if we failed return false otherwise return true
    failedRuleOption.map(_ => false).getOrElse(true)
  }
  private def makeHistoricData(accumulators: typedAccumulators, vl: ValidationListener) = {
    // Make the HistoricData object
    val historicDataBuilder = HistoricData.newBuilder()
    /*accumulatorsValues.map{case (key, value) =>
      historicDataBuilder.addUserCounters(
        CounterInfo.newBuilder().value(makeNumeric(value)).name(key).build())}
    vl.toMap().map{case (key, value) =>
      historicDataBuilder.addInternalCounters(
        CounterInfo.newBuilder().value(makePBNumeric(value)).name(key).build())}
     */
    historicDataBuilder.build()
  }
  private def saveCounters(historicData: HistoricData, success: Boolean) = {
    val prefix = success match {
      case true => "_SUCCESS"
      case false => "_FAILURE"
    }
    val historicDataBytes = historicData.toByteArray()
    val historicDataDebug = historicData.toString()
    sc.parallelize(List((null, historicDataBytes)))
      .saveAsSequenceFile(config.jobBasePath + "/" + config.jobDir + "/validator/HistoricDataSequenceFile" + prefix)
  }

  private def findOldCounters() = {
    List(HistoricData.newBuilder().build());
  }
}
