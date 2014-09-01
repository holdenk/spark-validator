/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

import com.holdenkarau.spark_validator.HistoricDataProtos.HistoricData

abstract class ValidationRule {
  def validate(historicData: List[HistoricData], vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean
}

abstract class AbsoluteValidationRule extends ValidationRule {
  override def validate(historicData: List[HistoricData], vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean = {
    validate(vl, counters)
  }
  def validate(vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean

}
