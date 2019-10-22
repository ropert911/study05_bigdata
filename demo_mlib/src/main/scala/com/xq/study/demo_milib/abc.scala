package com.xq.study.demo_milib

import com.xq.study.demo_milib.KMeansTest.do1
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/22
  */
object abc {
  def main(args: Array[String]): Unit = {
    //设置spark参数
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()

    do1(spark)

    spark.sparkContext.stop()
  }

  def do1(spark: SparkSession): Unit = {
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.evaluation.ClusteringEvaluator

    // Loads data.
    val dataset = spark.read.format("libsvm").load("C:\\Users\\sk-qianxiao\\Desktop\\data\\sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)
    println("中心数据: ")
    model.clusterCenters.foreach(println)

    // Make predictions
    val predictions = model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
  }
}
