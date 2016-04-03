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

  def getReadPath(jobBasePath: String, jobName: String, success: Boolean): String = {
    val status = success match {
      case true => "SUCCESS"
      case false => "FAILURE"
    }

    val readPath = s"$jobBasePath/$jobName/validator/HistoricDataParquet/status=$status"
    readPath
  }

  def getWritePath(jobBasePath: String, jobName: String, success: Boolean, date: String): String = {
    val readPath = getReadPath(jobBasePath, jobName, success)
    val writePath = s"$readPath/date=$date"
    writePath
  }

}
