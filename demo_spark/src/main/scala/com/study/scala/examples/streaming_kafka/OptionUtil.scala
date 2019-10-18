package com.study.scala.examples.streaming_kafka

import org.apache.commons.cli.{CommandLine, GnuParser, HelpFormatter, Options}

/**
  * @author sk-qianxiao
  * @date 2019/10/18
  */
object OptionUtil {
  /**
    * 获取配置参数
    *
    * @return
    */
  def getOptions(): Options = {
    val options = new Options
    options.addOption("a", "app-name", false, "app name")
    options.addOption("i", "interval", true, "interval time (seconds)")
    options.addOption("b", "bootstrap-servers", true, "kafka broker list")
    options.addOption("g", "group-id", false, "kafka group id")
    options.addOption("t", "topics", false, "the topic list of gpb")
    options.addOption("m", "max-rate", false, "maxRatePerPartition or messages per second for each partiton")
    options.addOption("z", "is-cluster", true, "single node is false,cluster is true")
    options.addOption("c", "checkpoint-path", false, "StreamingContext checkpoint path")
    options.addOption("h", "help", false, "help")
  }

  /**
    * 获取帮助信息
    *
    * @param opt
    */
  def usage(opt: Options): Unit = {
    val formatter = new HelpFormatter();
    formatter.printHelp("the information of DataMonitor :", opt);
  }

  /**
    * 进行参数解析
    *
    * @param args
    * @return
    */
  def parseArgs(args: Array[String]): CommandLine = {
    val options = OptionUtil.getOptions()
    val parser = new GnuParser
    val cmd = parser.parse(options, args)
    if (cmd.hasOption("help")) {
      OptionUtil.usage(options)
      System.exit(0)
    }

    cmd
  }
}
