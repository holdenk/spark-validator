/*
 * Configuration for the Spark Validator includes the job path and
 * rules.
 */
package com.holdenkarau.spark.validator

/**
 * Validation configuration that used to hold useful information that will be used during validation.
 *
 * @param jobBasePath Absolute path that will be used to store counters information and load old counters' data to
 *                    compare with current data.
 * @param jobName     A unique name that will be used in all validations of this job.
 * @param firstTime   Is it the first time to run this job or not.
 * @param rules       The required validation rules that needs to be validated.
 */
case class ValidationConf(jobBasePath: String, jobName: String, firstTime: Boolean, rules: List[ValidationRule]) {
}
