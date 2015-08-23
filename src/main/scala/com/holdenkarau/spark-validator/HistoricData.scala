/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

case class HistoricData(jobid: Long, counters: Map[String, Long])
