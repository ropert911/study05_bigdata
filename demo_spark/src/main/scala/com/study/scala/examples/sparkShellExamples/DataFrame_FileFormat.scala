package com.study.scala.examples.sparkShellExamples

import com.study.scala.examples.sparkShellExamples.DataFrameExamples_Shell.Person
import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/21
  */
object DataFrame_FileFormat {
  /**
    * 不同的格式加载
    */
  def test3(): Unit = {
    //以tex方式加载
    val filepath = "file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.txt"
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val peopleDF1 = spark.sparkContext.textFile(filepath).map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()

    //以json格式加载 出来的结果可能是 DataFrame = [age: bigint, name: string]
    val peopleDF2 = spark.read.format("json").load("file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json")
    val peopleDF3 = spark.read.json("file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json")
  }

  /**
    * parquet格式读取
    */
  def test4(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val parquetFileDF = spark.read.parquet("file:///usr/local/spark/examples/src/main/resources/users.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT * FROM parquetFile")
    namesDF.foreach(attributes => println("Name: " + attributes(0) + " favorite color:" + attributes(1)))
  }

  def save(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.json("file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json")

    //保存为不同的文件格式
    df.write.format("csv").save("file:///usr/local/spark/mycode/newpeople.csv") //保存为文件
    df.rdd.saveAsTextFile("file:///usr/local/spark/mycode/newpeople.txt") //保存为文件
    df.write.parquet("file:///usr/local/spark/mycode/newpeople.parquet") //存为列式文件
  }
}
