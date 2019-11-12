package com.xq.study.demo_milib

import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * 示例：K-means 聚类分析,使用欧式距离公式计算距离
  * Created By sk-tengfeiwang on 2017/10/19.
  */
object 聚类算法_KMeans {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    /** hadoop用户，这里默认是零 */
    println("HADOOP_USER_NAME==" + System.getProperty("HADOOP_USER_NAME"))

    /** spark用户名，在linux平台上执行和执行用户是一致的 */
    println("user.name==" + System.getProperty("user.name"))

    //设置spark参数
    val spark = SparkSession.builder().appName(聚类算法_KMeans.getClass.getName).master("local[1]").getOrCreate()

    test(spark, spark.sparkContext)

    spark.sparkContext.stop()
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {
    val parseData = getTrainData(sc)
    parseData.cache()

    /** 重新生成数据模型 */
    val numClusters = 7 //分类数
    val numIterations = 100 //迭代次数
    val clusters = KMeans.train(parseData, numClusters, numIterations)


    /** 所有点到中心点的距离的 平方和，一般来说越小越好，但这又有分类数形成一定的冲突 */
    val wssse = clusters.computeCost(parseData)
    println(s"与中心点的距离平方和 = $wssse")

    println("显示数据模型的中间点==")
    val centers = clusters.clusterCenters.toList
    for (index <- 0 until centers.size) {
      println(index + "\t" + centers(index))
    }

    /** 判断哪个类是异常类 */
    val clusterNumber = clusters.predict(parseData).map(cluster => (cluster, 1)).reduceByKey(_ + _).collect()
    clusterNumber.foreach(println)
    val abNormalClusterIndex = getAbNormalIndex(clusterNumber)

    parseData.unpersist()

    //判定新数据是不是合理
    checkData(clusters, abNormalClusterIndex, Array(35.2, 20))
    checkData(clusters, abNormalClusterIndex, Array(35.2, 20))
    checkData(clusters, abNormalClusterIndex, Array(16.7, 10))
    checkData(clusters, abNormalClusterIndex, Array(49.6, 26))
    checkData(clusters, abNormalClusterIndex, Array(49.6, 7))
    checkData(clusters, abNormalClusterIndex, Array(49.6, 100))
  }

  def checkData(clusters: KMeansModel, abNormalClusterIndex: Int, inputData: Array[Double]): Unit = {
    if (-1 != abNormalClusterIndex)
      if (abNormalClusterIndex == clusters.predict(Vectors.dense(inputData))) {
        println("异常数据 ")
      }
  }

  def getTrainData(sc: SparkContext): RDD[Vector] = {
    val buffer = new ListBuffer[String]
    val random = new Random()
    for (index <- 1 to 10000) {
      val range = if (index >= 9990) random.nextInt(15) else random.nextInt(2) + 1
      val times = if (index >= 9990) random.nextInt(20) + 20 else random.nextInt(3) + 20

      val str = +range + "," + times
      //      println(str)
      buffer += str
    }
    buffer += "50,50"

    val data = sc.parallelize(buffer)
    val parseData = data.map(x => Vectors.dense(x.split(",").map(_.toDouble)))
    parseData
  }

  def getAbNormalIndex(clusterNumber: Array[(Int, Int)]): Int = {
    var total: Int = 0
    var smallValue = clusterNumber(0)
    clusterNumber.foreach(value => {
      total = total + value._2
      if (value._2 < smallValue._2) {
        smallValue = value
      }
    })

    var clusterIndex = -1
    if (smallValue._2 / total < 0.05) {
      clusterIndex = smallValue._1
    }

    clusterIndex
  }

  def loadModel(sc: SparkContext, modelPath: String): KMeansModel = {
    try {
      val clusters = KMeansModel.load(sc, modelPath)
      println("加载进来的数据模型===>")
      val centers = clusters.clusterCenters.toList
      for (index <- 0 until centers.size) {
        println(index + "\t" + centers(index))
      }
      clusters
    } catch {
      case ex: Exception => {
        null
      }
    }
  }
}
