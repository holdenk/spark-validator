/*
 * Configuration for the Spark Validator includes the job path and
 * rules.
 */
package com.holdenkarau.spark_validator

case class ValidationConf (jobBasePath: String, jobDir: String, firstTime: Boolean, rules: List[ValidationRule]) {
}
