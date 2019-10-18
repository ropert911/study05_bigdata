package com.study.scala.examples

import org.apache.spark.SparkContext

/**
  * @author xq
  * @data 2019/10/18
  **/
object examples {
  def fileRRDD(sc: SparkContext): Unit = {
    //文件中的每一行成为一个元素 RDD[String]
    val lineRDD = sc.textFile("profile")
    //为RDD中的每个元素调用函数
    lineRDD.foreach(println)
  }

  def arrayRDD(sc: SparkContext): Unit = {
    val array = Array(1, 2, 3, 4, 5, 6)
    //数组转RDD[INT]
    val rdd = sc.parallelize(array)
    rdd.foreach(println)
  }
}
