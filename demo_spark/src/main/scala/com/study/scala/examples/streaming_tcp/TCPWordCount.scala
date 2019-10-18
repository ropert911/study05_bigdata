package com.study.scala.examples.streaming_tcp

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author sk-qianxiao
  * @date 2019/10/18
  */


object TCPWordCount {
  def main(args: Array[String]) {
    //设置log4j日志级别
    StreamingLogSet.setStreamingLogLevels()

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCountStateful")

    val sc = new StreamingContext(conf, Seconds(5))

    tcpListen(sc)

    sc.start()
    sc.awaitTermination()
  }

  //定义状态更新函数
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def tcpListen(sc: StreamingContext): Unit = {
    //设置检查点，检查点具有容错机制
    sc.checkpoint("file:///usr/local/spark/mycode/streaming/stateful/")

    //lines为 ReceiverInputDStream[String]，使用上和RDD[String]有点像
    val lines = sc.socketTextStream("localhost", 9999)
    //words 为  DStream[String]
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    //把DStream保存到文本文件中
    stateDstream.saveAsTextFiles("file:///usr/local/spark/mycode/streaming/dstreamoutput/output.txt")
  }
}
