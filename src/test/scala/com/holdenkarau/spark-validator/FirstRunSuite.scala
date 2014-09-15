/*
 * Verifys that the Spark Validator functions at least somewhat on first run
 */

package com.holdenkarau.spark_validator

import org.scalatest.{Assertions, BeforeAndAfterEach, FunSuite}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.file.Files;

class SparkContextSchedulerCreationSuite
  extends FunSuite {
  // TODO(holden): factor out a bunch of stuff but lets add a first test as a starting point
  test("null validation test") {
    val sc = new SparkContext("local", "test")
    val tempPath = Files.createTempDirectory(null).toString()
    val vc = new ValidationConf(tempPath, "1", true, List[ValidationRule]())
    val v = new Validation(sc, vc)
    sc.stop()
    assert(v.validate() === true)
  }
}
