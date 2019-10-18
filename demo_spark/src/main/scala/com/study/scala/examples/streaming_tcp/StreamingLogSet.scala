package com.study.scala.examples.streaming_tcp

import org.apache.spark.internal.Logging
import org.apache.log4j.{Level, Logger}

/**
  * @author sk-qianxiao
  * @date 2019/10/18
  */
object StreamingLogSet extends Logging {
  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
