package com.xq.study.demo_milib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 示例：K-means 聚类分析,使用欧式距离公式计算距离
  *
  * @author sk-qianxiao
  * @date 2019/3/21
  */
object Kmeans {
  def main(args: Array[String]) {
    //hadoop用户，这里默认是零
    println("HADOOP_USER_NAME==" + System.getProperty("HADOOP_USER_NAME"))
    //spark用户名，在linux平台上执行和执行用户是一致的
    println("user.name==" + System.getProperty("user.name"))


    val spark = SparkSession.builder().appName("batchFileWordCount_AppName").getOrCreate()
    val sc = spark.sparkContext

    do1(sc)

    sc.stop()
  }

  def do1(sc: SparkContext) {
    val logFile = "file:///root/input"
    val rdd = sc.textFile(logFile)


    println("=================内容==============")
    rdd.foreach(println)

    val rcRDD = rdd.map(_.split(" "))
    val sample = rcRDD.map(row => Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble)))
    sample.cache()

    //形成训练数据模型
    val numClusters = 5 //分类数
    val numIterations = 50 //最大迭代次数
    val clusters = KMeans.train(sample, numClusters, numIterations)

    //显示数据模型的中间点
    val centers = clusters.clusterCenters.toList
    for (index <- 0 until centers.size) {
      println(index + "\t" + centers(index))
    }

    //所有点到中心点的距离的 平方和，一般来说越小越好，但这又有分类数形成一定的冲突
    val wssse = clusters.computeCost(sample)
    println(s"与中心点的距离平方和 = $wssse")

    //predict生成的是RDD[INT], 是每个数所有数据模型的index
    //做map开始是为了做计数统计
    val r1 = clusters.predict(sample)
    r1.map(cluster => (cluster, 1)).reduceByKey(_ + _).collect().foreach(println _)

    sample.unpersist()
  }
}
