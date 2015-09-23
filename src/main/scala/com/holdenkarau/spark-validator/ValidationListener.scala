/*
 * Listener to collect Spark execution information.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable
import scala.collection.immutable

import org.apache.spark.scheduler._
import org.apache.spark.executor.TaskMetrics

class ValidationListener extends SparkListener {

  private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()
  private val stageMetrics = mutable.Buffer[StageInfo]()
  /**
   * Create a map representing the counters and info from this job
   */
  def toMap(): Map[String, Long] = {
    tasksToMap()
  }

  private def tasksToMap(): Map[String, Long] = {
    val tim = taskInfoMetrics.map{case (taskInfo, metrics) =>
      val keyPrefix = s"taskinfo.${taskInfo.taskId}.${taskInfo.attempt}"
      val kvs = Seq(("launchTime", taskInfo.launchTime),
        ("successful", taskInfo.successful match {
          case true => 1L
          case fasle => 0L
        }),
        ("duration" , taskInfo.duration)) ++ taskMetricsToMap(metrics)
      (keyPrefix, kvs)
    }
    // Aggregate the keys across all tasks
    val globals = tim.foldLeft(mutable.Map[String, Long]()){(acc, nv) =>
      nv._2.foreach{case (k, v) =>
        acc(k) = (acc.get(k).getOrElse(0L) + v)
      }
      acc}.toMap
    val per = tim.flatMap{case (keyPrefix, kvs) => kvs.map{case (k, v) => (keyPrefix + k , v)}}
    globals ++ per
  }

  private def taskMetricsToMap(metrics: TaskMetrics): Seq[(String, Long)] = {
    Seq(
      ("executorRunTime", metrics.executorRunTime),
      ("jvmGCTime", metrics.jvmGCTime),
      ("resultSerializationTime", metrics.resultSerializationTime),
      ("memoryBytesSpilled", metrics.memoryBytesSpilled),
      ("diskBytesSpilled", metrics.diskBytesSpilled)
    ) ++
    (metrics.inputMetrics match {
      case None => Seq(("noInputData", 1L))
      case Some(inputMetrics) => {
        Seq(
          ("noInputData", 0L),
          ("bytesRead", inputMetrics.bytesRead),
          ("recordsRead", inputMetrics.recordsRead))
      }
    }) ++
    (metrics.outputMetrics match {
      case None => Seq(("noOutputData", 1L))
      case Some(outputMetrics) => {
        Seq(
          ("noOutputData", 0L),
          ("bytesWritten", outputMetrics.bytesWritten),
          ("recordsWritten", outputMetrics.recordsWritten))
        }
    })
    // TODO: Shuffled read, shuffled write, update blocks.
  }
  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    stageMetrics += stageCompleted.stageInfo
  }

  /**
   * Called when a task ends
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
  }

  def copy(): ValidationListener = {
    val other = new ValidationListener()
    taskInfoMetrics.copyToBuffer(other.taskInfoMetrics)
    stageMetrics.copyToBuffer(other.stageMetrics)
    other
  }
}
