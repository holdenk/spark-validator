package com.holdenkarau.spark.validator

case class HistoricData(jobid: Long, counters: scala.collection.Map[String, Long])
