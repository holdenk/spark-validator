/*
 * Historic data for the SparkValidator. On disk represented as two separate sequence files.
 */
package com.holdenkarau.spark_validator

case class HistoricData(userCounters: Map[String, Numeric[_]], internalCounters: Map[String, Numeric[_]])
