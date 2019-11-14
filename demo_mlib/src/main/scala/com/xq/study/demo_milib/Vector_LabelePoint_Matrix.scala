package com.xq.study.demo_milib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object Vector_LabelePoint_Matrix {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(Vector_LabelePoint_Matrix.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    vectorTest(sc)
    vectorStatic(sc)
    LabeledPointTest(sc)
    MatrixTest(sc)
  }

  /**
    * Vector
    */
  def vectorTest(sc: SparkContext): Unit = {
    println("向量Vector===========================")
    //稠密向量：2，5，8
    val vd = Vectors.dense(2, 5, 8)
    println(vd(1))
    println(vd)

    //稀疏向量 个数，序号，序号对应的value
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 3, 5, 7))
    println(vs(0)) //序号访问
    println(vs)
  }

  def vectorStatic(sc: SparkContext): Unit ={
    println("向量统计===========================")
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
    println("L0范数：每列中的非零个数==>" + summary.numNonzeros)
    println("L1范数：向量中每个元素绝对值的和==>" + summary.normL1)
    println("L2范数：向量元素绝对值的平方和再开平方==>" + summary.normL2)
  }

  /**
    * LabeledPoint
    */
  def LabeledPointTest(sc: SparkContext): Unit = {
    println("LabeledPoint ===========================")
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println("稠密向量LabeledPoint==>" + pos)

    val neg = LabeledPoint(2.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    println("稀疏向量 LabeledPoint==>" + neg)

    val examples = MLUtils.loadLibSVMFile(sc, "C:\\Users\\sk-qianxiao\\Desktop\\data\\sample_kmeans_data.txt")
    println("加载出来的稀疏向量 LabeledPoint")
    examples.foreach(println)
  }

  /**
    * 矩阵的表示：使用密集矩阵和稀疏矩阵
    */
  def MatrixTest(sc: SparkContext): Unit = {
    println("密集矩阵 3行2列======>")
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)


    println("稀疏矩阵======>")
    //    * {{{
    //      *   1.0 0.0 4.0
    //      *   0.0 3.0 5.0
    //      *   2.0 0.0 6.0
    //      * }}}
    //    * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
    //    * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
    //Array(0, 2, 3, 6)  新col的序号,即在新的col中，row数组对应的序号， 个数是cols+1,最后一个的值是6，指的是values的总长度
    //Array(0, 2, 1, 0, 1, 2) 行入口, 个数和 values个数一致
    //使用时: (0,2,1,0,1,2) 把0放到0后--0,0  1放到第2个数1后--1,1   2放到第3个数0后--0,2
    //((0,0),2,(1,1),(0,2),1,2)
    val sm: Matrix = Matrices.sparse(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    println(sm)
  }
}
