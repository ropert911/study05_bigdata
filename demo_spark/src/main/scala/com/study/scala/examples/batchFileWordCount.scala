package com.study.scala.examples

import org.apache.spark.sql.SparkSession

/**
  * 示例：统计文件单词个数
  *
  * @author sk-qianxiao
  * @date 2019/3/21
  */
object batchFileWordCount {
  def main(args: Array[String]) {
    //hadoop用户，这里默认是零
    println("HADOOP_USER_NAME==" + System.getProperty("HADOOP_USER_NAME"))
    //spark用户名，在linux平台上执行和执行用户是一致的
    println("user.name==" + System.getProperty("user.name"))


    val spark = SparkSession.builder().appName("batchFileWordCount_AppName").getOrCreate()
    val sc = spark.sparkContext

    val logFile = "hdfs://node1:9000/profile"
    val rdd = sc.textFile(logFile)
    rdd.cache()

    println("=================内容==============")
    rdd.foreach(println)

    println("=================打印单词统计==============")
    val rdd2 = rdd.flatMap(_.split(" ")).map((_, 1))
    val wordcount = rdd2.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    wordcount.foreach(println)

    rdd.unpersist()
    sc.stop()
  }
}
