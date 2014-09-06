/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

abstract class ValidationRule {
  def validate(historicData: List[HistoricData], vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean
  /*
   * Check that a range is valid. One or both of min and max should be specified. If neither is specified
   * returns true */
  def checkRange(input: Numeric[_], min: Option[Numeric[_]], max: Option[Numeric[_]]): Boolean = {
    val res = (min.map(minValue => input > minValue) ++ max.map(maxValue => input < maxValue))
    res.fold(true)(a, b => a && b)
  }
}

abstract class NoHistoryValidationRule extends ValidationRule {
  override def validate(historicData: List[HistoricData], vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean = {
    validate(vl, counters)
  }
  def validate(vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean
}

case class AbsoluteSparkCounterValidationRule(counterName: String, min: Option[Numeric[_]], max: Option[Numeric[_]]) extends NoHistoryValidationRule {
  override def validate(vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean = {
  }
}

case class AbsoluteUserCounterValidationRule(counterName: String, min: Option[Numeric[_]], max: Option[Numeric[_]]) extends NoHistoryValidationRule {
  override def validate(vl: ValidationListener, counters: scala.collection.Map[String, Numeric[_]]): Boolean = {
  }
}
