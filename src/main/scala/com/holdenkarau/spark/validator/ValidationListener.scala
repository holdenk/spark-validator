/*
 * Listener to collect Spark execution information.
 */
package com.holdenkarau.spark.validator

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable

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
    val tim = taskInfoMetrics.map { case (taskInfo, metrics) =>
      val keyPrefix = s"taskinfo.${taskInfo.taskId}.${taskInfo.attemptNumber}"
      val kvs = Seq(("launchTime", taskInfo.launchTime),
        ("successful", taskInfo.successful match {
          case true => 1L
          case false => 0L
        }),
        ("duration", taskInfo.duration)) ++ taskMetricsToMap(metrics)
      (keyPrefix, kvs)
    }
    // Aggregate the keys across all tasks
    val globals = tim.foldLeft(mutable.Map[String, Long]()) { (acc, nv) =>
      nv._2.foreach { case (k, v) =>
        acc(k) = (acc.getOrElse(k, 0L) + v)
      }
      acc
    }.toMap
    val per = tim.flatMap {
      case (keyPrefix, kvs) =>
        kvs.map { case (k, v) => (keyPrefix + k, v) }
    }
    globals ++ per
  }

  private def taskMetricsToMap(metrics: TaskMetrics): Seq[(String, Long)] = {
    val inputMetrics = metrics.inputMetrics
    val outputMetrics = metrics.outputMetrics
    val shuffleReadMetrics = metrics.shuffleReadMetrics
    val shuffleWriteMetrics = metrics.shuffleWriteMetrics
    Seq(
      ("executorRunTime", metrics.executorRunTime),
      ("jvmGCTime", metrics.jvmGCTime),
      ("resultSerializationTime", metrics.resultSerializationTime),
      ("memoryBytesSpilled", metrics.memoryBytesSpilled),
      ("diskBytesSpilled", metrics.diskBytesSpilled)
    ) ++
    Seq(
      ("bytesRead", inputMetrics.bytesRead),
      ("recordsRead", inputMetrics.recordsRead)) ++
    Seq(
      ("bytesWritten", outputMetrics.bytesWritten),
      ("recordsWritten", outputMetrics.recordsWritten)) ++
    Seq(
      ("shuffleRemoteBlocksFetched", shuffleReadMetrics.remoteBlocksFetched),
      ("shuffleRemoteLocalBlocksFetched", shuffleReadMetrics.localBlocksFetched),
      ("shuffleLocalBytesRead", shuffleReadMetrics.localBytesRead),
      ("shuffleRemoteBytesRead", shuffleReadMetrics.remoteBytesRead))
    // TODO shuffle write
  }

  /**
   * Called when a stage completes successfully or fails,
   * with information on the completed stage.
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
