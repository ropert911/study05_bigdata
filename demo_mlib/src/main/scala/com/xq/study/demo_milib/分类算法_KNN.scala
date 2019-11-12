package com.xq.study.demo_milib

import com.xq.study.demo_milib.algorithm.TF_IDF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * @author sk-qianxiao
  * @date 2019/11/12
  */
object 分类算法_KNN {
  val logger = LoggerFactory.getLogger(分类算法_KNN.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(TF_IDF.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    test(spark, sc)
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {
    val sample: Array[Array[Any]] = Array(
      Array(1, "宝贝当家", 45, 2, 9, "喜剧片"),
      Array(2, "美人鱼", 21, 17, 5, "喜剧片"),
      Array(3, "澳门风云3", 54, 9, 11, "喜剧片"),
      Array(4, "功夫熊猫3", 39, 0, 31, "喜剧片"),
      Array(5, "谍影重重", 5, 2, 57, "动作片"),
      Array(6, "叶问3", 3, 2, 65, "动作片"),
      Array(7, "伦敦陷落", 2, 3, 55, "动作片"),
      Array(8, "我的特工爷爷", 6, 4, 21, "动作片"),
      Array(9, "奔爱", 7, 46, 4, "爱情片"),
      Array(10, "夜孔雀", 9, 39, 8, "爱情片"),
      Array(11, "代理情人", 9, 38, 2, "爱情片"),
      Array(12, "新步步惊心", 8, 34, 17, "爱情片"))
    // 求唐人街办案类型
    val movie = Array(13, "唐人街探案", 23, 3, 17, null);
    val length = sample.length - 1;
    println("序号 名称        距离");
    var movieDisList = List[MovieDis]();

    for (i <- 0 to length) {
      val mv: Array[Any] = sample(i);
      val distances: Double = getDistance(mv, movie);
      val movieDis = new MovieDis(mv(0).asInstanceOf[Int], mv(1).asInstanceOf[String], distances, mv(5).asInstanceOf[String]);
      println(printf("%s %s %s", mv(0), mv(1), distances));
      //列表添加跟java不一样，坑
      movieDisList = (movieDisList.+:(movieDis))
    }
    movieDisList = movieDisList.sortWith((o1: MovieDis, o2: MovieDis) => (o1.distance < o2.distance));

    var k: Int = 5;
    println("按照欧式距离排序，取k=5");
    movieDisList = movieDisList take 5;
    movieDisList.foreach { o => println(o) }
  }

  def getDistance(movie1: Array[Any], movie2: Array[Any]): Double = {
    val ps1 = Array(movie1(2).asInstanceOf[Integer].doubleValue(), movie1(3).asInstanceOf[Integer].doubleValue(), movie1(4).asInstanceOf[Integer].doubleValue());
    val ps2 = Array(movie2(2).asInstanceOf[Integer].doubleValue(), movie2(3).asInstanceOf[Integer].doubleValue(), movie2(4).asInstanceOf[Integer].doubleValue());
    return getDistance(ps1, ps2);
  }

  def getDistance(ps1: Array[Double], ps2: Array[Double]): Double = {
    if (ps1.length != ps1.length) {
      throw new RuntimeException("属性数量不对应");
    }
    val length = ps1.length - 1;
    var total: Double = 0;
    for (i <- 0 to length) {
      val sub = ps1(i) - ps2(i);
      total = total + (sub * sub);
    }

    return Math.sqrt(total);
  }
}