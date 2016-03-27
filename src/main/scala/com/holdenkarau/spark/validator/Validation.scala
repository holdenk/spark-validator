/*
 * Make it easy to check counters after our Spark job runs.
 */
package com.holdenkarau.spark.validator

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable.HashMap

/**
 * Validation class that will be used to validate counters data.
 *
 * @param sqlContext
 * @param config validation configurations.
 */
class Validation(sqlContext: SQLContext, config: ValidationConf) {

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
  sqlContext.sparkContext.addSparkListener(validationListener)

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

  // TODO Add API returning list of failed rules

  /**
   * Validates a run of a Spark Job. Returns true if the job is valid and
   * also adds a SUCCESS marker to the path specified. Takes in a jobid.
   * jobids should be monotonically increasing and unique per job.
   *
   * @param jobid Current job id. jobids should be monotonically increasing and unique per job.
   * @return Returns true if the job is valid, false if not valid.
   */
  def validate(jobid: Long): Boolean = {
    // Make a copy of the validation listener so that if we trigger
    // any work on the spark context this does not update our counters from this point
    // forward.
    val validationListenerCopy = validationListener.copy()

    // Also fetch all the accumulators values
    val oldRuns = findOldHistoricData()
    // Format the current data
    val currentData = makeHistoricData(jobid, accumulators, validationListenerCopy)
    // Run through all of our rules until one fails
    val failedRules = config.rules.flatMap(rule => rule.validate(oldRuns, currentData))
    if (failedRules.nonEmpty) {
      println("Failed rules include " + failedRules)
    }
    // if we failed return false otherwise return true
    val result = failedRules.isEmpty
    saveCounters(currentData, result)
    result
  }

  /**
   * Converts both Spark counters & user counters into a HistoricData object
   */
  private def makeHistoricData(id: Long, accumulators: typedAccumulators, vl: ValidationListener): HistoricData = {
    val counters = accumulators.doubles.map { case (key, value) =>
      (key, value.value.toLong) // We loose info, but w/e
    } ++
    accumulators.floats.map { case (key, value) =>
      (key, value.value.toLong) // We loose info, but w/e
    } ++
    accumulators.longs.map { case (key, value) =>
      (key, value.value)
    } ++
    accumulators.ints.map { case (key, value) =>
      (key, value.value.toLong) // We loose info, but w/e
    } ++
      vl.toMap().map { case (key, value) =>
        (key, value) // spark metrics
    }

    HistoricData(id, counters.toMap)
  }

  /**
   * Saves historic data to the path given in validationConf.
   *
   * @param success success flag, true if job validation succeeded, false if validation failed.
   */
  private def saveCounters(historicData: HistoricData, success: Boolean): Unit = {
    val prefix = success match {
      case true => "SUCCESS"
      case false => "FAILURE"
    }

    // creates accumulator DataFrame
    val schema = StructType(List(StructField("counterName", StringType, false),
      StructField("value", LongType, false)))
    val rows = sqlContext.sparkContext.parallelize(historicData.counters.toList).map(kv => Row(kv._1, kv._2))
    val data = sqlContext.createDataFrame(rows, schema)

    // save accumulators DataFrame
    val id = historicData.jobid
    val path = config.jobBasePath + "/" + config.jobName +
      "/validator/HistoricDataParquet/status=" + prefix + "/id=" + id
    data.write.parquet(path)
  }

  /**
   * Gets the Historic Data as an Array.
   */
  private def findOldHistoricData(): Array[HistoricData] = {
    val countersDF = loadOldCounters()
    countersDF match {
      case Some(df) => {
        val historicDataRDD = df.select("id", "counterName", "value").rdd
          .map { row => (
            try {
              row.getLong(0)
            } catch {
              case e: Exception => row.getInt(0).toLong
            },
            (row.getString(1), row.getLong(2)))
          }
          .groupByKey()
          .map { case (jobId, counters) => HistoricData(jobId, counters.toMap) }

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
    val base = config.jobBasePath + "/" + config.jobName
    val path = base + "/validator/HistoricDataParquet/status=SUCCESS"
    // Spark SQL doesn't handle empty directories very well...
    val fs = org.apache.hadoop.fs.FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      val inputDF = sqlContext.read.parquet(path)
      Some(inputDF)
    } else {
      None
    }
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
   * @param sc Spark Context.
   * @param config Validation configurations.
   */
  def apply(sc: SparkContext, config: ValidationConf): Validation = {
    new Validation(new SQLContext(sc), config)
  }
}
