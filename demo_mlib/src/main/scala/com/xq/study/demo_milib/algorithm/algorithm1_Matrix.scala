package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.聚类算法_KMeans
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object algorithm1_Matrix {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(聚类算法_KMeans.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    summarystatisticsTest(sc)
  }

  /**
    * 矩阵的统计值：列平均，列方差，列非零个数
    */
  def summarystatisticsTest(sc: SparkContext): Unit = {
    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(4.0, 15.0, 200.0),
        Vectors.dense(4.0, 20.0, 300.0)
      )
    )

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("列最大值==>" + summary.max)
    println("列最小值==>" + summary.min)
    println("列平均值==>" + summary.mean) //每列平均值
    //列方差  方差等于各个数据与其算术平均数的离差平方和的平均数(平均时个数要减1) 如第1列平均为3 （4+1=1）/2=3
    println("列方差==>" + summary.variance)
    //L0范数指向量中非零元素的个数
    println("L0范数每列中的非零个数==>" + summary.numNonzeros)
    //L1范数：向量中每个元素绝对值的和
    println("L1范数==>" + summary.normL1)
    //L2范数：向量元素绝对值的平方和再开平方
    println("L2范数==>" + summary.normL2)
  }


}
