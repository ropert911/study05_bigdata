package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.聚类算法_KMeans
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * spearman: 进行等级划分后线性相关关系
  *
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object algorithm3_spearman {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(聚类算法_KMeans.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    relationTest(sc)
  }

  /**
    * 相关性计算spearman: 进行等级划分后线性相关关系，如年龄和身高；水深和水压
    *
    * @param sc
    */
  def relationTest(sc: SparkContext): Unit = {
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD
    {
      //第二个数量是等比的
      val seriesX: RDD[Double] = sc.parallelize(Array(1, 3, 5, 7, 9, 11))
      val seriesY: RDD[Double] = sc.parallelize(Array(2, 4, 8, 16, 32, 64))

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
      val correlation1 = Statistics.corr(seriesX, seriesY, "pearson")
      val correlation2 = Statistics.corr(seriesX, seriesY, "spearman")

      println(s"pearson_相关性: $correlation1")
      println(s"spearman_相关性: $correlation2")
    }
  }
}
