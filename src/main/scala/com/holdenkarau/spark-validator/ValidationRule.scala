/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

import org.apache.spark.sql._

abstract class ValidationRule {
  def validate(historicData: List[HistoricData], current: HistoricData): Boolean
}

abstract class NoHistoryValidationRule extends ValidationRule {
  override def validate(historicData: List[HistoricData], current: HistoricData): Boolean = {
    validate(current)
  }
  def validate(current: HistoricData): Boolean
}

case class AbsoluteSparkCounterValidationRule(counterName: String,
  min: Option[Long], max: Option[Long]) extends NoHistoryValidationRule {
  override def validate(current: HistoricData): Boolean = {
    println("Validating on "+current)
    val value = current.counters.map(x => (x.name, x.value)).toMap.get(counterName).get.asInstanceOf[Long]
    min.map(_ < value).getOrElse(true) && max.map(value < _).getOrElse(true)
  }
}
