package com.xq.study.demo_milib.algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。该模型将每个词语映射到一个固定大小的向量。word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
  *
  * 在下面的代码段中，我们首先用一组文档，其中每一个文档代表一个词语序列。对于每一个文档，我们将其转换为一个特征向量。此特征向量可以被传递到一个学习算法。
  * 参考：https://blog.csdn.net/liulingyuan6/article/details/53390949
  *
  * @author sk-qianxiao
  * @date 2019/11/11
  */
object 分类算法_SVMWithSGD {
  val logger = LoggerFactory.getLogger(分类算法_SVMWithSGD.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(TF_IDF.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    test(spark, sc)
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {
    // 加载和解析数据文件
    val data = sc.textFile("C:\\Users\\sk-qianxiao\\Desktop\\sample_svm_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
    }

    /** 设置迭代次数并进行进行训练 */
    val numIterations = 20
    val model = SVMWithSGD.train(parsedData, numIterations)

    /** 清除阈值，以便“predict”将输出原始预测分数 */
//    model.clearThreshold()

    /** 统计分类错误的样本比例 */
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds.foreach(item => println(item._1 + "  " + item._2))
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
    println("Training Error = " + trainErr)
  }
}
