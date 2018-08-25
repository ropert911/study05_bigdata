package spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sk-qianxiao on 2018/8/24.
 */

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "/user/root/input/hdfs-site.xml"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    //flatMap 每一行。  split分出单词    map映射成(key,value)    reduceByKey根据key做分组 review，参数是value
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}