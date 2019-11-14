package com.xq.study.demo_milib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/11/14
  */
object Apriori先验算法 {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(Apriori先验算法.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    test1(sc)
  }

  val rate = 0.75

  def test1(sc: SparkContext): Unit = {
    //得到输入数据RDD
    val myDat = sc.parallelize(
      Seq(
        Array(1, 3, 4, 5),
        Array(2, 3, 5),
        Array(1, 2, 3, 4, 5),
        Array(2, 3, 4, 5)
      )
    )

    val C1 = myDat.flatMap(x => x).distinct().collect() //distinct()是去重操作，对应C1=createC1(myDat) #得到1项集 #[1, 2, 3, 4, 5],
    //将输入数据RDD转化为set的列表 #[{1, 3, 4, 5}, {2, 3, 5}, {1, 2, 3, 4, 5}, {2, 3, 4, 5}]
    val D = myDat.map(x => x.toSet).collect()
    val D_bc = sc.broadcast(D)

    val length = myDat.collect().length

    //K=1时进行筛选
    val k1Result = sc.parallelize(C1).map(x => {
      var num = 0
      for (d1 <- D) {
        if (Set(x).subsetOf(d1)) {
          num += 1
        }
      }
      (x, num.toDouble / length)
    }).filter(_._2 >= rate).map(_._1).collect().sortWith((i1, i2) => i1 < i2)
    k1Result.foreach(println)


    //K=2时进行筛选
    k1Result.foreach(item => {
      getKMore(D, length, k1Result, Set(item))
    })
  }

  /**
    *
    * @param D        所有结果集
    * @param length   结果集的个数
    * @param k1Result 维1满足的列表
    * @param setK
    */
  def getKMore(D: Array[Set[Int]], length: Int, k1Result: Array[Int], setK: Set[Int]): Unit = {
    var last = -1
    setK.foreach(item => {
      val index = k1Result.indexOf(item)
      if (index > last) {
        last = index
      }
    })

    for (i <- last + 1 to k1Result.length - 1) {
      val setK_1 = setK + k1Result(i)
      var num = 0
      for (d1 <- D) {
        if (setK_1.subsetOf(d1)) {
          num += 1
        }
      }
      if ((num.toDouble / length) >= rate) {
        println("K+1 enable==>" + setK_1)
        //只要最后一个元素还没有到最后一个数，就可能有下一阶
        if (i != k1Result.length - 1) {
          getKMore(D, length, k1Result, setK_1)
        }
      }
    }
  }
}
