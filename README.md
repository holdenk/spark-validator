# Spark Validator

A library you can include in your Spark job to validate the counters and perform operations on success.

## Why you should validate counters

Maybe you are really lucky and you never have intermitent outages or bugs in your code.

If you have counters for things like records processed or number of errors, its really easy to write bounds for these. Even if you don't have custom counters you can use Spark's built in counters and by looking at historic values we can establish reasonable bounds. This can help catch jobs which fail to process some of your records. This is not a replacement for unit or integration testing.

### How spark validation works

We store all of the counters from each run.

If a run is successful it will run your on success handler. If you just want to mark the run as success you can specify a file for spark validator to touch. 

### How to write your validation rules

#### Absolute

#### Relative

## How to build

sbt - Remember when it was called the simple build tool?

sbt/sbt compile

## How to use

### Scala

### Java

### Python

## License

