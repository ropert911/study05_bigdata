package com.xq.study.demo_milib.algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 词频－逆向文件频率（TF-IDF）
  * 参考： https://blog.csdn.net/liulingyuan6/article/details/53390949
  *
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object TF_IDF {
  val logger = LoggerFactory.getLogger(t1.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(TF_IDF.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    test(spark, sc)
    //一个示例
    test2(spark, sc)
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {
    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    sentenceData.show()

    /** 把句子转单词 */
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()
    val abc = wordsData.randomSplit(Array(0.7,0.3))

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    //setNumFeatures(100)表示将Hash分桶的数量设置为100个，这个值默认为2的20次方，即1048576，可以根据你的词语数量来调整，一般来说，这个值越大，不同的词被计算为一个Hash值的概率就越小，数据也更准确，但需要消耗更大的内存
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show()
//    featurizedData.foreach(row => println(row(2), row(3)))
    //(WrappedArray(hi, i, heard, about, spark),(20,[0,5,9,17],[1.0,1.0,1.0,2.0]))  意思：hi, 用23表示，在文档中出现了1次
    // CountVectorizer 也可获取词频向量

    /** 计算TF-IDF值 */
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()
//    rescaledData.foreach(row => println(row(2), row(3), row(4)))
    rescaledData.select("features", "label").take(3).foreach(println)
  }

  case class RawDataRecord(category: String, text: String)
  def test2(spark: SparkSession, sc: SparkContext): Unit = {
    var srcRDD = sc.textFile("/tmp/lxw1234/sougou/").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category",$"text",$"words").take(1)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select($"category", $"words", $"rawFeatures").take(1)


    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData.select($"category", $"features").take(1)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")
    trainDataRdd.take(1)

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, 1.0, "multinomial")

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

  }
}
