package com.xq.study.demo_milib

import org.apache.spark.mllib.linalg.Vectors

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object Vector_Test {
  def main(args: Array[String]) {
    val vd = Vectors.dense(2, 5, 8)
    println(vd(1))
    println(vd)

    //向量个数，序号，value
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 3, 5, 7))
    println(vs(0)) //序号访问
    println(vs)

    //稀疏矩阵
    val vs2 = Vectors.sparse(4, Array(0, 2, 1, 3), Array(9, 3, 5, 7))
    println(vs2(2))
    println(vs2)
  }
}
