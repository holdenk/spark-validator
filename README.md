[![buildstatus](https://travis-ci.org/holdenk/spark-validator.svg?branch=master)](https://travis-ci.org/holdenk/spark-validator)
[![codecov.io](http://codecov.io/github/holdenk/spark-validator/coverage.svg?branch=master)](http://codecov.io/github/holdenk/spark-validator?branch=master)

# Spark Validator

A library you can include in your Spark job to validate the counters and perform operations on success.

This software should be considered pre-alpha.

## Why you should validate counters

Maybe you are really lucky and you never have intermitent outages or bugs in your code.

If you have accumulators for things like records processed or number of errors, its really easy to write bounds for these. Even if you don't have custom counters you can use Spark's built in metrics (bytes read, time, etc.) and by looking at historic values we can establish reasonable bounds. This can help catch jobs which fail to process some of your records. This is not a replacement for unit or integration testing.

### How spark validation works

We store all of the metrics from each run along with all of the accumulators you pass in.

If a run is successful it will run your on success handler. If you just want to mark the run as success you can specify a file for spark validator to touch. 

### How to write your validation rules

#### Absolute

#### Relative

## How to build

sbt - Remember when it was called the simple build tool?

sbt/sbt compile

## How to use

### Scala

At the start of your Spark program once you have constructed your spark context call
[code]
import com.holdenkarau.spark_validator
...
val vl
[/code]

### Java

vNext

### Python

vNext+1

## License

