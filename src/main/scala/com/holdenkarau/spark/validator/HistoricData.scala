package com.holdenkarau.spark.validator

import java.sql.Timestamp

import scala.collection.immutable.Seq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession, functions}

case class HistoricData(
  counters: scala.collection.Map[String, Long], date: Timestamp) {
  /**
   * Saves historic data to the given path.
   */
  def saveHistoricData(session: SparkSession, path: String): Unit = {
    // creates accumulator DataFrame
    val schema = StructType(List(
      StructField("counterName", StringType, false),
      StructField("value", LongType, false)))
    val rows =
      session.sparkContext.parallelize(counters.toList).
        map(kv => Row(kv._1, kv._2))
    val data = session.createDataFrame(rows, schema)

    // save accumulators DataFrame
    val writePath = s"$path/date=$date"
    data.write.parquet(writePath)
  }

}

object HistoricData {

  /**
   * Converts both Spark counters & user counters into a HistoricData object
   */
  def apply(
    accumulators: TypedAccumulators, vl: ValidationListener, date: Timestamp):
      HistoricData = {
    val counters = accumulators.toMap() ++ vl.toMap()
    HistoricData(counters, date)
  }

  /**
   * Gets the Historic Data as an Array.
   */
  def loadHistoricData(session: SparkSession, path: String): Array[HistoricData] = {
    import session.implicits._
    val countersDF = loadHistoricDataDataFrame(session, path)
    countersDF match {
      case Some(df) =>
        val countersDataset = df.select(
          df("date"), functions.map(df("counterName"), df("value")).alias("counter"))
          .groupBy(df("date"))
          .agg(functions.collect_list("counter").alias("counters"))
        val result = countersDataset.map(x =>
          (HistoricData(
            // fields are ordered ny name
            x.getSeq[Map[String, Long]](1).reduceLeft(_ ++ _),
            x.getTimestamp(0)
          )))
        result.collect()
      case None =>
        new Array[HistoricData](0)
    }
  }

  /**
   * Returns a DataFrame of the old counters (for SQL funtimes).
   */
  private def loadHistoricDataDataFrame(
    session: SparkSession, path: String): Option[DataFrame] = {

    // Spark SQL doesn't handle empty directories very well...
    val hadoopConf = session.sparkContext.hadoopConfiguration
    val fs =
      org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      val inputDF = session.read.parquet(path)
      Some(inputDF)
    } else {
      None
    }
  }

  def getPath(jobBasePath: String, jobName: String, success: Boolean): String = {
    val status = success match {
      case true => "SUCCESS"
      case false => "FAILURE"
    }

    val path = s"$jobBasePath/$jobName/validator/HistoricDataParquet/status=$status"
    path
  }

}
