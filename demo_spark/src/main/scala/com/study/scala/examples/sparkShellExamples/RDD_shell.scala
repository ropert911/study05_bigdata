package com.study.scala.examples.sparkShellExamples

import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * @author sk-qianxiao
  * @date 2019/10/22
  */
object RDD_shell {
  def makeRDD(): Unit = {
    val spark = SparkSession.builder().appName(RDD_shell.getClass.getName).master("local[1]").getOrCreate()
    val buffer = new ListBuffer[String]
    val random = new Random()
    for (index <- 1 to 1000) {
      val range = if (index >= 990) random.nextInt(15) else random.nextInt(2) + 1
      val times = if (index >= 990) random.nextInt(20) + 20 else random.nextInt(3) + 20

      val str = +range + "," + times
      println(str)
      buffer += str
    }

    val data = spark.sparkContext.parallelize(buffer)
  }

  def makeRDD2(sc: SparkContext): Unit ={
    val array = Array(1, 2, 3, 4, 5, 6)
    //数组转RDD[INT]
    val rdd = sc.parallelize(array)
  }

  def makeRDD3(sc: SparkContext): Unit ={
    val textFile1 = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
  }
}
