/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark.validator

import scala.collection.IndexedSeq

abstract class ValidationRule {
  /**
   * Return None for success and Some("Error") for error
   */
  def validate(historicData: IndexedSeq[HistoricData], current: HistoricData): Option[String]
}

abstract class NoHistoryValidationRule extends ValidationRule {
  override def validate(historicData: IndexedSeq[HistoricData], current: HistoricData): Option[String] = {
    validate(current)
  }

  /**
   * Return None for success and Some("Error") for error
   */
  def validate(current: HistoricData): Option[String]
}

/**
 * Helper class to make it easy to write a rule based on previous average
 * maxDiff is an absolute
 */
case class AvgRule(counterName: String,
    maxDiff: Double, histLength: Option[Int], newCounter: Boolean = false) extends ValidationRule {

  override def validate(historicData: IndexedSeq[HistoricData], current: HistoricData):
  Option[String] = {
    val samples = histLength.filter(_ <= 0).map(historicData.take(_)).getOrElse(historicData)
    val data = samples.flatMap(_.counters.get(counterName))

    data.toList match {
      case Nil => {
        if (newCounter) {
          None
        } else {
          Some("No data found for " + counterName + " and was not marked as new counter")
        }
      }
      case head :: tail => {
        val avg = tail.foldLeft((head.toDouble, 1.0))((r: (Double, Double), c: Long) =>
          ((r._1 + (c.toDouble / r._2)) * r._2 / (r._2 + 1), r._2 + 1))._1

        val value = current.counters.get(counterName).get
        if (Math.abs(value - avg)<= maxDiff) {
          None
        } else {
          Some(s"Value $value for counter $counterName was not in the range of " +
            s"avg $avg+/- tol $maxDiff")
        }
      }
    }
  }
}


/**
 * Helper class to make it easy to write  rule with an absolute min/max.
 * Note: assumes that the key is present.
 */
case class AbsoluteSparkCounterValidationRule(counterName: String,
  min: Option[Long], max: Option[Long]) extends NoHistoryValidationRule {

  override def validate(current: HistoricData): Option[String] = {
    val option = current.counters.get(counterName)
    if (option.isDefined) {
      val value = option.get
      if (min.forall(_ < value) && max.forall(value < _)) {
        None
      } else {
        Some(s"Value $value was not in range $min, $max")
      }
    } else {
      Some(s"Failed to find key $counterName in ${current.counters}")
    }
  }
}

/**
 * Helper class to make it easy to write a two counter relative
 * rule with an absolute min/max. Note: assumes that the keys is present.
 */
case class AbsolutePercentageSparkCounterValidationRule(numeratorCounterName: String, denominatorCounterName: String,
    min: Option[Double], max: Option[Double]) extends NoHistoryValidationRule {

  override def validate(current: HistoricData): Option[String] = {
    val numeratorOption = current.counters.get(numeratorCounterName)
    val denominatorOption = current.counters.get(denominatorCounterName)

    if (numeratorOption.isDefined && denominatorOption.isDefined) {
      val value = numeratorOption.get.toDouble / denominatorOption.get.toDouble
      if (min.forall(_ < value) && max.forall(value < _)) {
        None
      } else {
        Some(s"Value $value was not in range $min, $max")
      }
    } else {
      Some(s"Failed to find keys $numeratorCounterName, $denominatorCounterName in " +
        s" ${current.counters}")
    }
  }
}
