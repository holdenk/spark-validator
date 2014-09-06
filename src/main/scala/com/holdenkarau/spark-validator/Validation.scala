/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

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
  /*
   * Saves the counters out as sequence & text files. Text files are for human debugging and
   * sequence files are for future runs of the validation program */
  private def saveCounters(accumulatorsValues: Map[String, Numeric[_]], vl: ValidationListener, pass: Boolean): Boolean = {
    // Make the HistoricData object
    val prefix = pass match {
      case true => "_SUCCESS"
      case false => "_FAILURE"
    }
    val userData = sc.parallelize(accumulatorsValues.toSeq)
    userData.saveAsSequenceFile(conf.jobBasePath + "/" + conf.jobDir + "/validator/HistoricUserDataSequenceFile" + prefix)
    userData.saveTextFile(conf.jobBasePath + "/" + conf.jobDir + "/validator/HistoricUserDataTextFile" + prefix)
    val internalData = sc.parallelize(vl.toMap().toSeq)
    internalData.saveAsSequenceFile(conf.jobBasePath + "/" + conf.jobDir + "/validator/HistoricInternalDataSequenceFile" + prefix)
    internalData.saveAsTextFile(conf.jobBasePath + "/" + conf.jobDir + "/validator/HistoricInternalDataTextFile" + prefix)
  }

  private def findOldCounters() = {
    List(HistoricData(null, null));
  }
}
