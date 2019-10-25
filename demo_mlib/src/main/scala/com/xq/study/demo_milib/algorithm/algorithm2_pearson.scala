package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.KMeansTest
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Pearson 适合等距及等比 公司 与y=a+bx 之间的匹配度，就是看不是是在一条直线上
  *
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object algorithm2_pearson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    relationTest(sc)
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

    {
      //下面的符合 y=6+2x
      val seriesX: RDD[Double] = sc.parallelize(Array(1, 3, 5, 7, 9, 11))
      val seriesY: RDD[Double] = sc.parallelize(Array(8, 12, 16, 20, 24, 28))

      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
      println(s"相关性11：: $correlation")
    }

    {
      //第二个数量是等比的
      val seriesX: RDD[Double] = sc.parallelize(Array(1, 3, 5, 7, 9, 11))
      val seriesY: RDD[Double] = sc.parallelize(Array(2, 4, 8, 16, 32, 64))

      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
      println(s"相关性22：: $correlation")
    }

    //    {
    //      val seriesX: RDD[Double] = sc.parallelize(Array(1, 10, 100))
    //      // a series must have the same number of partitions and cardinality as seriesX
    //      val seriesY: RDD[Double] = sc.parallelize(Array(2.0, 20.0, 200))
    //
    //      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
    //      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    //      println(s"Correlation is: $correlation")
    //    }
    //
    //    {
    //      val seriesX: RDD[Double] = sc.parallelize(Array(1, 10, 100))
    //      // a series must have the same number of partitions and cardinality as seriesX
    //      val seriesY: RDD[Double] = sc.parallelize(Array(2.0, 11.0, 200))
    //
    //      // 使用Pearson方法计算相关性。输入"spearman"作为Spearman的方法。如果未指定方法，则默认使用Pearson方法。
    //      val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    //      println(s"Correlation is: $correlation")
    //    }
  }
}
