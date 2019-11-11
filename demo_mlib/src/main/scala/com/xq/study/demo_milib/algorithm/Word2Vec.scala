package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.algorithm.TF_IDF.{test, test2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.ml.feature.Word2Vec

/**
  * Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。该模型将每个词语映射到一个固定大小的向量。word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
  *
  * 在下面的代码段中，我们首先用一组文档，其中每一个文档代表一个词语序列。对于每一个文档，我们将其转换为一个特征向量。此特征向量可以被传递到一个学习算法。
  * 参考：https://blog.csdn.net/liulingyuan6/article/details/53390949
  *
  * @author sk-qianxiao
  * @date 2019/11/11
  */
object Word2Vec {
  val logger = LoggerFactory.getLogger(PipeLine.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(TF_IDF.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    test(spark, sc)
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF.show()

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("text", "result").take(3).foreach(println)
  }
}
