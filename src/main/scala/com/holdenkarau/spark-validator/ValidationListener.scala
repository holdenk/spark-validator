/*
 * Listener to collect Spark execution information.
 */
package com.holdenkarau.spark_validator

import scala.collection.mutable

import org.apache.spark.scheduler._
import org.apache.spark.executor.TaskMetrics

class ValidationListener extends SparkListener {

  val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()
  val stageMetrics = mutable.Buffer[StageInfo]()
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
}
