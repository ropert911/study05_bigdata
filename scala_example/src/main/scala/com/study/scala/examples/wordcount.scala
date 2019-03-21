package com.study.scala.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sk-qianxiao on 2019/3/21.
  */
object wordcount {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "root")  //如果不设置，就默认就是sk-qianxiao，是没有权限的
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

//    val logFile = "hdfs://192.168.20.51:9000/pyspark/script/default/ac_statistics.py"   //这里因为nginx转的问题，引起datanode ip被识别错
    val logFile = "hdfs://192.168.20.101:8020/pyspark/script/default/ac_statistics.py"
    val rdd = sc.textFile(logFile)
    println("=================内容==============")
    rdd.foreach(println)

    println("=================word count==============")
    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    wordcount.foreach(println)
    wordcount.saveAsTextFile("hdfs://192.168.20.101:8020/pyspark/script/default/result")

    sc.stop()
  }
}
