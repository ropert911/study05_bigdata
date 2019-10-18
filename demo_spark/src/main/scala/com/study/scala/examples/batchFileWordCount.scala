package com.study.scala.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sk-qianxiao on 2019/3/21.
  */
object batchFileWordCount {
  def main(args: Array[String]) {
    //hadoop用到的:如果不设置，就默认就是sk-qianxiao，没有权限的
    //    System.setProperty("HADOOP_USER_NAME", "hdfs")
    //spark用到的
    //    System.setProperty("user.name", "hdfs")

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
