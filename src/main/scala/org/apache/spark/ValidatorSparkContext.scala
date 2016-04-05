package org.apache.spark

import org.apache.spark.sql.SQLContext

object ValidatorSparkContext {
  private val WAIT_TIMEOUT_MILLIS = 10000

  /**
   * Give the Context some time to finish adding metrics in ValidationListener.
   */
  def waitUntilEmpty(sqlContext: SQLContext, timeout: Int = WAIT_TIMEOUT_MILLIS) =
    sqlContext.sparkContext.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
}
