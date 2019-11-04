package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.KMeansTest
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SparkSession

/**
  *
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object temperature {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    relationTest(sc)
  }

  /**
    * 相关性计算spearman: 进行等级划分后线性相关关系，如年龄和身高；水深和水压
    *
    * @param sc
    */
  def relationTest(sc: SparkContext): Unit = {
    val observations = sc.parallelize(
      //模拟过去5天的数据
      Seq(
        Vectors.dense(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25),
        Vectors.dense(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25),
        Vectors.dense(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25),
        Vectors.dense(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25),
        Vectors.dense(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25)

      )
    )
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("列平均值==>" + summary.mean) //每列平均值

    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD
    {
      //第二个数量是等比的
      val seriesX: RDD[Double] = sc.parallelize(summary.mean.toArray)
      val seriesY: RDD[Double] = sc.parallelize(Array(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 47, 50, 47, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25))

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
      val correlation1 = Statistics.corr(seriesX, seriesY, "pearson")
      val correlation2 = Statistics.corr(seriesX, seriesY, "spearman")

      println(s"pearson_相关性: $correlation1")
      println(s"spearman_相关性: $correlation2")
    }
  }
}
