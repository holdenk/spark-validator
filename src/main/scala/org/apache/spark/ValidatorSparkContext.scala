package org.apache.spark

import org.apache.spark.sql.SparkSession

object ValidatorSparkContext {
  private val WAIT_TIMEOUT_MILLIS = 10000

  /**
   * Give the Context some time to finish adding metrics in ValidationListener.
   */
  def waitUntilEmpty(session: SparkSession, timeout: Int = WAIT_TIMEOUT_MILLIS) =
    session.sparkContext.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
}
