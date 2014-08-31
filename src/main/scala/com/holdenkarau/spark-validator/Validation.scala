/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

class Validation(sc: SparkContext) {
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
   * @param sc a spark context to run on (not necessarily the same as the job).
   *  create it by calling setupListener.
   * @param config the configuration (path, rules, etc.) for this job
   */
  def validate(sc: SparkContext, config: ValidationConf): Boolean = {
    false
  }
}
