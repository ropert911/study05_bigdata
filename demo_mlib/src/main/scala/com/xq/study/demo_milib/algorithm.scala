package com.xq.study.demo_milib

import com.xq.study.demo_milib.Vector_Test.MatrixTest
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object algorithm {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    summarystatisticsTest(sc)
  }

  /**
    * 矩阵的统计值：列平均，列方差
    */
  def summarystatisticsTest(sc: SparkContext): Unit = {
    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 15.0, 200.0),
        Vectors.dense(3.0, 20.0, 300.0)
      )
    )

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean) //每列平均值
    //列方差  方差等于各个数据与其算术平均数的离差平方和的平均数(平均时个数要减1)
    println(summary.variance)
    println(summary.numNonzeros) // 每列中的非零个数 number of nonzeros in each column
  }

  /**
    * 相关性计算
    *
    * @param sc
    */
  def relationTest(sc: SparkContext): Unit = {
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD
    {
      val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))
      // a series must have the same number of partitions and cardinality as seriesX
      val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 55))

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
      println(s"Correlation is: $correlation")
    }

    {
      val data: RDD[Vector] = sc.parallelize(
        Seq(
          Vectors.dense(1.0, 10.0, 100.0),
          Vectors.dense(2.0, 20.0, 200.0),
          Vectors.dense(3.0, 30.0, 300.0),
          Vectors.dense(4.0, 60.0, 500.0))

      ) // note that each Vector is a row and not a column
      data.foreach(println)

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
      val correlMatrix: Matrix = Statistics.corr(data, "pearson")
      println(correlMatrix.toString)
    }
  }
}
