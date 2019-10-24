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

    //    summarystatisticsTest(sc)
    relationTest(sc)
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

  /**
    * 相关性计算Pearson:评估定距变量间的线性相关关系，如年龄和身高；水深和水压
    *
    * @param sc
    */
  def relationTest(sc: SparkContext): Unit = {
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD
    {
      val seriesX: RDD[Double] = sc.parallelize(Array(1, 10, 100))
      // a series must have the same number of partitions and cardinality as seriesX
      val seriesY: RDD[Double] = sc.parallelize(Array(2.0, 20.0, 200))

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
      println(s"Correlation is: $correlation")
    }

    {
      val data: RDD[Vector] = sc.parallelize(
        Seq(
          Vectors.dense(1.0, 10.0, 100.0, 300),
          Vectors.dense(2.0, 20.0, 200.0, 400),
          Vectors.dense(3.0, 20.0, 200.0, 400))

      ) // note that each Vector is a row and not a column
      data.foreach(println)

      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法
      //计算相关矩阵，相关矩阵第i行第j列的元素是原矩阵第i列和第j列的相关系数
      val corrMatrix: Matrix = Statistics.corr(data, "pearson")
      println(corrMatrix.toString)
    }
  }
}
