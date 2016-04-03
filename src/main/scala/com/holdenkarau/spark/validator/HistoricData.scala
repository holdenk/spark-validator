package com.holdenkarau.spark.validator

import java.sql.Timestamp

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class HistoricData(counters: scala.collection.Map[String, Long]) {
  /**
   * Saves historic data to the given path.
   */
  def saveHistoricData(sqlContext: SQLContext, path: String): Unit = {
    // creates accumulator DataFrame
    val schema = StructType(List(
      StructField("counterName", StringType, false),
      StructField("value", LongType, false)))
    val rows = sqlContext.sparkContext.parallelize(counters.toList).map(kv => Row(kv._1, kv._2))
    val data = sqlContext.createDataFrame(rows, schema)

    // save accumulators DataFrame
    data.write.parquet(path)
  }

}

object HistoricData {

  /**
   * Converts both Spark counters & user counters into a HistoricData object
   */
  def apply(accumulators: TypedAccumulators, vl: ValidationListener): HistoricData = {
    val counters = accumulators.toMap() ++ vl.toMap()
    HistoricData(counters)
  }

  /**
   * Gets the Historic Data as an Array.
   */
  def loadHistoricData(sqlContext: SQLContext, path: String): Array[HistoricData] = {
    val countersDF = loadHistoricDataDataFrame(sqlContext, path)
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
  private def loadHistoricDataDataFrame(sqlContext: SQLContext, path: String): Option[DataFrame] = {
    // Spark SQL doesn't handle empty directories very well...
    val fs = org.apache.hadoop.fs.FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      val inputDF = sqlContext.read.parquet(path)
      Some(inputDF)
    } else {
      None
    }
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
