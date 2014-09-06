/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

import com.holdenkarau.spark_validator.HistoricDataProtos.HistoricData

class Validation(sc: SparkContext, config: ValidationConf) {
  protected val accumulators = new HashMap[String, Accumulator[Numeric[_]]]()
  protected val validationListener = new ValidationListener()
  sc.addSparkListener(validationListener)

  /*
   * Register an accumulator with the SparkValidator. Will overwrite any previous
   * accumulator with the same name.
   */
  def registerAccumulator(accumulator: Accumulator[Numeric[_]],
    name: String) {
    accumulators += ((name, accumulator))
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
    val accumulatorsValues = accumulators.mapValues(_.value)
    val oldRuns = findOldCounters()
    config.rules.exists(rule => rule.validate(oldRuns, validationListenerCopy, accumulatorsValues))
  }
  private def saveCounters(): Boolean = {
    // Make the HistoricData object
    val historicDataBuilder = HistoricData.newBuilder()
    accumulators.map{case (key, value) =>
      historicDataBuilder.addUserCounters(
        CounterInfo.newBuilder.value(value).name(key).build())}
    validationListener.toMap().map{case (key, value) =>
      historicDataBuilder.addInternalCounters(
        CounterInfo.newBuilder.value(value).name(key).build())}
    val historicData = historicDataBuilder.build()
    val historicDataByes = historicData.toByteArray()
    val historicDataDebug = historicData.toString()
    sc.parallelize(List(nil, historicDataBytes))
      .saveAsSequenceFile(conf.jobBasePath + "/" + conf.jobDir + "/validator/HistoricDataSequenceFile")
  }
  private def findOldCounters() = {
    List(HistoricData.newBuilder().build());
  }
}
