/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark.validator

import java.time.temporal.{ChronoField, TemporalField}

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
 * Helper class to make it easy to compare the current counter value
 * with its average value from previous runs. If the difference between current
 * value and average is greater than maxDiff, then this rule will not be valid.
 *
 * @param counterName   Counter name we are validating. If counterName doesn't exist, rule will not be valid.
 * @param maxDiff       Maximum allowed difference between current value and average value from previous runs.
 * @param historyLength Length to compare with from previous runs. If the history is too long, you can limit the
 *                      number of runs you comparing with, by this length. If history length is None or negative
 *                      value all previous runs will be included in comparison.
 * @param freq          Day to compare with for ex. DAY_OF_WEEK, DAY_OF_MONTH, ... etc.
 * @param newCounter    Flag that indicates if this run is the first time for this counter or not.
 *                      Average rule is a relative rule that compares counter value with previous runs,
 *                      So if newCounter is true no comparison will occur.
 */
class AbstractAverageRule(counterName: String, maxDiff: Double, historyLength: Option[Int],
    freq: Option[TemporalField], newCounter: Boolean = false) extends ValidationRule {

  override def validate(historicData: IndexedSeq[HistoricData], current: HistoricData):
  Option[String] = {
    val samples = historyLength.filter(_ <= 0).map(historicData.take(_)).getOrElse(historicData)
    val counterData = samples.filter(_.counters.contains(counterName))

    if (counterData.isEmpty) {
      if (newCounter) {
        return None
      } else {
        return Some(s"No data found for $counterName and was not marked as new counter")
      }
    }

    val filteredData = freq.map(day =>
      counterData.filter(_.date.get(day) == current.date.get(day))
     ).getOrElse(counterData)
      .map(_.counters.get(counterName).get)

    filteredData.toList match {
      case Nil => {
        None
      }
      case head :: tail => {
        val avg = tail.foldLeft((head.toDouble, 1.0))((r: (Double, Double), c: Long) =>
          ((r._1 + (c.toDouble / r._2)) * r._2 / (r._2 + 1), r._2 + 1))._1

        val value = current.counters.get(counterName).get
        if (Math.abs(value - avg) <= maxDiff) {
          None
        } else {
          Some(s"Value $value for counter $counterName was not in the range of avg $avg +/- tol $maxDiff")
        }
      }
    }
  }
}

/**
 * Helper class to make it easy to compare the current counter value
 * with its average value from previous runs. If the difference between current
 * value and average is greater than maxDiff, then this rule will not be valid.
 *
 * @param counterName   Counter name we are validating. If counterName doesn't exist, rule will not be valid.
 * @param maxDiff       Maximum allowed difference between current value and average value from previous runs.
 * @param historyLength Length to compare with from previous runs. If the history is too long, you can limit the
 *                      number of runs you comparing with, by this length. If history length is None or negative
 *                      value all previous runs will be included in comparison.
 * @param newCounter    Flag that indicates if this run is the first time for this counter or not.
 *                      Average rule is a relative rule that compares counter value with previous runs,
 *                      So if newCounter is true no comparison will occur.
 */
case class AverageRule(counterName: String, maxDiff: Double, historyLength: Option[Int],
    newCounter: Boolean = false)
  extends AbstractAverageRule(counterName, maxDiff, historyLength, None, newCounter)

/**
 * Helper class to make it easy to compare the current counter value with its average value
 * from previous runs in the same day of the week for ex. MONDAY, TUESDAY, ... etc.
 * If the difference between current value and average is greater than maxDiff, then this rule will not be valid.
 *
 * @param counterName   Counter name we are validating. If counterName doesn't exist, rule will not be valid.
 * @param maxDiff       Maximum allowed difference between current value and average value from previous runs.
 * @param historyLength Length to compare with from previous runs. If the history is too long, you can limit the
 *                      number of runs you comparing with, by this length. historyLength limits the whole runs not
 *                      runs on the same day of the week. If history length is None or negative
 *                      value all previous runs will be included in comparison.
 * @param newCounter    Flag that indicates if this run is the first time for this counter or not.
 *                      Average rule is a relative rule that compares counter value with previous runs,
 *                      So if newCounter is true no comparison will occur.
 */
case class AverageRuleSameWeekDay(counterName: String, maxDiff: Double, historyLength: Option[Int],
    newCounter: Boolean = false)
  extends AbstractAverageRule(counterName, maxDiff, historyLength, Some(ChronoField.DAY_OF_WEEK), newCounter)

/**
 * Helper class to make it easy to compare the current counter value with its average value
 * from previous runs in the same day of the month for ex. 1st day, 5th day, ... etc.
 * If the difference between current value and average is greater than maxDiff, then this rule will not be valid.
 *
 * @param counterName   Counter name we are validating. If counterName doesn't exist, rule will not be valid.
 * @param maxDiff       Maximum allowed difference between current value and average value from previous runs.
 * @param historyLength Length to compare with from previous runs. If the history is too long, you can limit the
 *                      number of runs you comparing with, by this length. historyLength limits the whole runs not
 *                      runs on the same day of the month. If history length is None or negative
 *                      value all previous runs will be included in comparison.
 * @param newCounter    Flag that indicates if this run is the first time for this counter or not.
 *                      Average rule is a relative rule that compares counter value with previous runs,
 *                      So if newCounter is true no comparison will occur.
 */
case class AverageRuleSameMonthDay(counterName: String, maxDiff: Double, historyLength: Option[Int],
    newCounter: Boolean = false)
  extends AbstractAverageRule(counterName, maxDiff, historyLength, Some(ChronoField.DAY_OF_MONTH), newCounter)


/**
 * Helper class to make it easy to limit counter value by absolute minimum and maximum values.
 * If counter value is less than min value or greater than max value, then this rule will not be valid.
 *
 * @param counterName Counter name we are validating. If counterName doesn't exist, rule will not be valid.
 * @param min         The minimum allowed value for this counter. If min is None, min limit will be ignored.
 * @param max         The maximum allowed value for this counter. If max is None, max limit will be ignored.
 */
case class AbsoluteValueRule(counterName: String, min: Option[Long], max: Option[Long])
  extends NoHistoryValidationRule {

  override def validate(current: HistoricData): Option[String] = {
    val option = current.counters.get(counterName)
    if (option.isDefined) {
      val value = option.get
      if (min.forall(_ <= value) && max.forall(_ >= value)) {
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
 * Helper class to make it easy to limit the ratio between two counters by min percentage and max percentage.
 * Percentage is calculated by dividing numeratorCounter over denominatorCounter. If percentage is less than
 * min value or greater than max value, then this rule will not be valid.
 *
 * @param numeratorCounterName   Numerator counter name we are validating. If numeratorCounterName doesn't exist,
 *                               rule will not be valid.
 * @param denominatorCounterName Denominator counter name we are validating. if denominatorCounterName doesn't
 *                               exist, rule will not be valid.
 * @param min                    The minimum allowed percentage. If min is None, min limit will be ignored.
 * @param max                    The maximum allowed percentage. If max is None, max limit will be ignored.
 */
case class AbsolutePercentageRule(numeratorCounterName: String, denominatorCounterName: String,
    min: Option[Double], max: Option[Double]) extends NoHistoryValidationRule {

  override def validate(current: HistoricData): Option[String] = {
    val numeratorOption = current.counters.get(numeratorCounterName)
    val denominatorOption = current.counters.get(denominatorCounterName)

    if (numeratorOption.isDefined && denominatorOption.isDefined) {
      val value = numeratorOption.get.toDouble / denominatorOption.get.toDouble
      if (min.forall(_ <= value) && max.forall(_ >= value)) {
        None
      } else {
        Some(s"Value $value was not in range $min, $max")
      }
    } else {
      Some(s"Failed to find keys $numeratorCounterName, $denominatorCounterName in ${current.counters}")
    }
  }
}
