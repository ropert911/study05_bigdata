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
    val spark = SparkSession.builder().appName(聚类算法_KMeans.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    //    vectorTest(sc)
    //    LabeledPointTest(sc)
    MatrixTest(sc)
  }

  /**
    * Vector
    */
  def vectorTest(sc: SparkContext): Unit = {
    //稠密向量：2，5，8
    val vd = Vectors.dense(2, 5, 8)
    println(vd(1))
    println(vd)

    //稀疏向量 个数，序号，序号对应的value
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 3, 5, 7))
    println(vs(0)) //序号访问
    println(vs)
  }

  /**
    * LabeledPoint
    */
  def LabeledPointTest(sc: SparkContext): Unit = {
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

    println("稠密向量 LabeledPoint")
    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println(pos)

    println("稀疏向量 LabeledPoint") //个数，序号，序号对应的value
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
  def MatrixTest(sc: SparkContext): Unit = {
    println("密集矩阵，按列的顺序======")
    import org.apache.spark.mllib.linalg.{Matrices, Matrix}
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)


    println("稀疏矩阵======")
    //    * {{{
    //      *   1.0 0.0 4.0
    //      *   0.0 3.0 5.0
    //      *   2.0 0.0 6.0
    //      * }}}
    //    * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
    //    * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
    //Array(0, 2, 3, 6)  新col的序号,即在新的col中，row数组对应的序号， 个数是cols+1,最后一个的值是3，指的是values的总长度
    //Array(0, 2, 1, 0, 1, 2) 行入口, 个数和 values个数一致
    //使用时: (0,2,1,0,1,2) 把0放到0后--0,0  1放到第2个数1后--1,1   2放到第3个数0后--0,2
    //((0,0),2,(1,1),(0,2),1,2)
    val sm: Matrix = Matrices.sparse(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    println(sm)
  }
}
