package com.holdenkarau.spark.validator

case class HistoricData(counters: scala.collection.Map[String, Long])

object HistoricData {

  /**
   * Converts both Spark counters & user counters into a HistoricData object
   */
  def apply(accumulators: typedAccumulators, vl: ValidationListener): HistoricData = {
    val counters = accumulators.toMap() ++ vl.toMap()
    HistoricData(counters)
  }
}
