package com.study.scala.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 本stream会起一个client连接到 9999端口
  * 服务器可以用 nc -lk 9999 方式启动，并在里面进行数据输入
  *
  * @author sk-qianxiao
  * @date 2019/10/21
  */
object streamingTcpWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("streamingTcpWordCount_AppName")
    val sc = new StreamingContext(conf, Seconds(10))
    sc.sparkContext.setLogLevel("INFO")

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
    sc.checkpoint("file:///root/spark-streaming-checkpoint")

    //lines为 ReceiverInputDStream[String]，使用上和RDD[String]有点像
    val lines = sc.socketTextStream("localhost", 9999)
    //words 为  DStream[String]
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    //把DStream保存到文本文件中
    stateDstream.saveAsTextFiles("file:///root/output.txt")
  }
}
