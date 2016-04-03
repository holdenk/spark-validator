package com.holdenkarau.spark.validator

import org.apache.spark.Accumulator

import scala.collection.mutable.HashMap

case class TypedAccumulators(
  doubles: HashMap[String, Accumulator[Double]],
  ints: HashMap[String, Accumulator[Int]],
  floats: HashMap[String, Accumulator[Float]],
  longs: HashMap[String, Accumulator[Long]]) {

  def toMap(): Map[String, Long] = {
    val accumulatorsMap: scala.collection.mutable.Map[String, Long] =
      doubles.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      } ++
      floats.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      } ++
      longs.map { case (key, value) =>
        (key, value.value)
      } ++
      ints.map { case (key, value) =>
        (key, value.value.toLong) // We loose info, but w/e
      }

    accumulatorsMap.toMap
  }
}
