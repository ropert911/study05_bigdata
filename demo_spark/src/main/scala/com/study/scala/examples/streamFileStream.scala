package com.study.scala.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author sk-qianxiao
  * @date 2019/10/21
  */
object streamFileStream {
  def main(args: Array[String]) {
    //设置为本地运行模式，2个线程，一个监听，另一个处理数据
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(20))// 时间间隔为20秒
    val lines = ssc.textFileStream("file:///home/xiaoqian/logfile")  //这里采用本地文件，当然你也可以采用HDFS文件
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

//参考： http://dblab.xmu.edu.cn/blog/1385-2/
//Spark Streaming每隔20秒就监听一次。
// 程序只监听目录下在程序启动后新增的文件，不会去处理历史上已经存在的文件。
