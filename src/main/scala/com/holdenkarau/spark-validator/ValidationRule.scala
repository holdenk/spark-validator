/*
 * Validation rules for the SparkValidator. If you want to add your own rules just extend Rule
 */
package com.holdenkarau.spark_validator

abstract class ValidationRule {
  def validate(a: String): Boolean
}
