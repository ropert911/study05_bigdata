package com.xq.study.demo_milib.algorithm

import com.xq.study.demo_milib.KMeansTest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @author sk-qianxiao
  * @date 2019/10/23
  */
object PipeLine {
  val logger = LoggerFactory.getLogger(PipeLine.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(KMeansTest.getClass.getName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    test(spark, sc)
  }

  /**
    * 管道示例
    */
  def test(spark: SparkSession, sc: SparkContext): Unit = {
    // Prepare training documents from a list of (id, text, label) tuples.
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    /** LogisticRegression 固定使用features列 */
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    //    model.write.overwrite().save("/tmp/spark-logistic-regression-model")
    //    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    //    pipeline.write.overwrite().save("/tmp/unfit-lr-model")
    //    val samePipLine = Pipeline.load("/tmp/unfit-lr-model")


    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")


    val a = model.transform(test)
    a.show()
    // Make predictions on test documents.
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }
}
