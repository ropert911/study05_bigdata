package com.xq.study.demo_milib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object Vector_Test {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    summarystatisticsTest(sc)
  }

  /**
    * Vector
    */
  def vectorTest(): Unit = {
    //稠密向量：2，5，8
    val vd = Vectors.dense(2, 5, 8)
    println(vd(1))
    println(vd)

    //稀疏向量 个数，序号，value
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 3, 5, 7))
    println(vs(0)) //序号访问
    println(vs)
  }

  /**
    * LabeledPoint
    */
  def LabeledPointTest(): Unit = {
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

    println("稠密向量 LabeledPoint")
    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println(pos)

    println("稀疏向量 LabeledPoint")
    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    println(neg)

    //    println("从文件中加载 LabeledPoint")
    //    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    //    val sc = spark.sparkContext
    //    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "C:\\Users\\sk-qianxiao\\Desktop\\data\\sample_kmeans_data.txt")
    //    examples.foreach(println)
  }

  /**
    * 矩阵的表示：使用密集矩阵和稀疏矩阵
    */
  def MatrixTest(): Unit = {
    println("密集矩阵，按列的顺序======")
    import org.apache.spark.mllib.linalg.{Matrices, Matrix}
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)
    println("稀疏矩阵======")
    // Create a sparse matrix (
    //    (0,0) 9.0
    //    (2,1) 6.0
    //    (1,1) 8.0
    // Array(0, 1, 3)中的3是用来说明有多少个数据
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println(sm)
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
    //列方差  方差等于各个数据与其算术平均数的离差平方和的平均数
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
