/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

import org.apache.spark.sql._

abstract class ValidationRule {
  def validate(historicData: List[HistoricData], current: HistoricData): Boolean
}

abstract class NoHistoryValidationRule[T] extends ValidationRule {
  override def validate(historicData: List[HistoricData], current: HistoricData): Boolean = {
    validate(current)
  }
  def validate(current: HistoricData): Boolean
}

case class AbsoluteSparkCounterValidationRule[T <: Numeric[_]](counterName: String, min: Option[T], max: Option[T]) extends NoHistoryValidationRule {
  override def validate(current: HistoricData): Boolean = {
    false
  }
}
