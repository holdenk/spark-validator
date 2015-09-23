/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

import scala.collection.IndexedSeq

import org.apache.spark.sql._

abstract class ValidationRule {
  def validate(historicData: IndexedSeq[HistoricData], current: HistoricData): Boolean
}

abstract class NoHistoryValidationRule extends ValidationRule {
  override def validate(historicData: IndexedSeq[HistoricData], current: HistoricData): Boolean = {
    validate(current)
  }
  def validate(current: HistoricData): Boolean
}

/**
 * Helper class to make it easy to write a rule based on previous average
 * maxDiff is an absolute
 */
case class AvgRule(counterName: String,
  maxDiff: Double, histLength: Option[Int], newCounter: Boolean=false) extends ValidationRule {
  override def validate(historicData: IndexedSeq[HistoricData], current: HistoricData): Boolean = {
    val samples = histLength.map(historicData.take(_)).getOrElse(historicData)
    val data = if (!newCounter) {
      samples.map(_.counters.get(counterName).get)
    } else {
      samples.flatMap(_.counters.get(counterName))
    }

    data.toList match {
      case Nil => newCounter
      case head :: tail => {
      val avg = tail.foldLeft((head.toDouble, 1.0))((r: (Double, Double), c: Long) =>
        ((r._1 + (c.toDouble/r._2)) * r._2 / (r._2+1), r._2+1))._1
      val value = current.counters.get(counterName).get.asInstanceOf[Long]
      (avg-maxDiff < value) && (value < avg+maxDiff)
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
  override def validate(current: HistoricData): Boolean = {
    val option = current.counters.get(counterName)
    val value = option.get.asInstanceOf[Long]
    min.map(_ < value).getOrElse(true) && max.map(value < _).getOrElse(true)
  }
}
