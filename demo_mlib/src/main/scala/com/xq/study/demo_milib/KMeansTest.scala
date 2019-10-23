package com.xq.study.demo_milib

import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * 示例：K-means 聚类分析,使用欧式距离公式计算距离
  * Created By sk-tengfeiwang on 2017/10/19.
  */
object KMeansTest {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //hadoop用户，这里默认是零
    println("HADOOP_USER_NAME==" + System.getProperty("HADOOP_USER_NAME"))
    //spark用户名，在linux平台上执行和执行用户是一致的
    println("user.name==" + System.getProperty("user.name"))

    //设置spark参数
    //val conf = new SparkConf().setAppName(TestKMeans.getClass.getName)
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()

    /*val data = Seq(Person("kebe",38),Person("james",30))

    val dfData = spark.createDataset(data)
    dfData.explain()
    dfData.show(20,false)
    dfData.printSchema()
    dfData.explain(true)

    val sf = new StructField("name",StringType,true)
    println(sf.name+"_"+sf.dataType+"_"+sf.nullable+"_"+sf.metadata)
    val struct = StructType(sf::Nil)
    println(struct("name"))*/

    do1(spark.sparkContext)

    spark.sparkContext.stop()
  }

  def do1(sc: SparkContext): Unit = {
    val list = List("11.0,33", "12.20,25", "25.6,34", "27.8,37", "16,33.4", "35,48.7", "21.3,46.8", "24.65,59.5", "17.5,67.3", "5.6,16.7")

    val buffer = new ListBuffer[String]
    val random = new Random()
    for (index <- 1 to 1000) {
      val range = if (index >= 990) random.nextInt(15) else random.nextInt(2) + 1
      val times = if (index >= 990) random.nextInt(20) + 20 else random.nextInt(3) + 20

      val str = +range + "," + times
      println(str)
      buffer += str
    }

    val data = sc.parallelize(buffer)
    val parseData = data.map(x => Vectors.dense(x.split(",").map(_.toDouble)))
    parseData.cache()

    //形成训练数据模型
    val numClusters = 5 //分类数
    val numIterations = 50 //迭代次数
    val clusters = KMeans.train(parseData, numClusters, numIterations)


    //所有点到中心点的距离的 平方和，一般来说越小越好，但这又有分类数形成一定的冲突
    val wssse = clusters.computeCost(parseData)
    println(s"与中心点的距离平方和 = $wssse")

    //predict生成的是RDD[INT], 是每个数所有数据模型的index
    //做map开始是为了做计数统计
    clusters.predict(parseData).map(cluster => (cluster, 1)).reduceByKey(_ + _).collect().foreach(println _)

    println("显示数据模型的中间点==")
    val centers = clusters.clusterCenters.toList
    for (index <- 0 until centers.size) {
      println(index + "\t" + centers(index))
    }


    //判定新数据是不是合理
    println(clusters.predict(Vectors.dense(Array(/*8.5,*/ 35.2, 20))))
    println(clusters.predict(Vectors.dense(Array(/*6.5,*/ 16.7, 10))))
    println(clusters.predict(Vectors.dense(Array(/*26.5,*/ 49.6, 26))))
    println(clusters.predict(Vectors.dense(Array(/*35.5,*/ 49.6, 7))))
    println(clusters.predict(Vectors.dense(Array(/*35.5,*/ 49.6, 100))))

    parseData.unpersist()
  }
}
