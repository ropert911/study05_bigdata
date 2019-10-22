package com.xq.study.demo_milib

import java.util.Random

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  *
  * Created By sk-tengfeiwang on 2017/10/19.
  */
object TestKMeans {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //设置spark参数
    //val conf = new SparkConf().setAppName(TestKMeans.getClass.getName)
    val spark = SparkSession.builder().appName(TestKMeans.getClass.getName).master("local[1]").getOrCreate()

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

    val list = List("11.0,33", "12.20,25", "25.6,34", "27.8,37", "16,33.4", "35,48.7", "21.3,46.8", "24.65,59.5", "17.5,67.3", "5.6,16.7")

    val buffer = new ListBuffer[String]
    val random = new Random()
    for (index <- 1 to 1000) {
      val range = if (index >= 990) random.nextInt(15) else random.nextInt(2)+1
      val times = if (index >= 990) random.nextInt(20)+20 else random.nextInt(3) + 20

      val str =  + range + "," + times
      println(str)
      buffer += str
    }

    val data = spark.sparkContext.parallelize(buffer)
    val parseData = data.map(x => Vectors.dense(x.split(",").map(_.toDouble)))

    val numClusters = 5
    val numIterations = 50
    val clusters = KMeans.train(parseData, numClusters, numIterations)

    val wssse = clusters.computeCost(parseData)
    println(s"within set sum of squared errors = $wssse")

    //clusters.clusterCenters.foreach(println(_))
    val centers = clusters.clusterCenters.toList
    for (index <- 0 until centers.size) {
      println(index + "\t" + centers(index))
    }
    println(clusters.predict(Vectors.dense(Array(/*8.5,*/ 35.2, 20))))
    println(clusters.predict(Vectors.dense(Array(/*6.5,*/ 16.7, 10))))
    println(clusters.predict(Vectors.dense(Array(/*26.5,*/ 49.6, 26))))
    println(clusters.predict(Vectors.dense(Array(/*35.5,*/ 49.6, 7))))

    clusters.predict(parseData).map(cluster => (cluster, 1)).reduceByKey(_ + _).collect().foreach(println _)
  }
}
