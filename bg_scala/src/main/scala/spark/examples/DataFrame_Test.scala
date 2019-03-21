package spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by sk-qianxiao on 2018/8/24.
  */

object DataFrame_Test {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.json("file:/opt/spark/examples/src/main/resources/people.json")

    df.printSchema() //打印表结构
    df.select(df("name"), df("age") + 1).show() //select
    df.filter(df("age") > 20).show() //过滤
    df.groupBy("age").count().show() //分组计数
    df.distinct().count()         //去重操作
    df.sort(df("age").desc).show() //按字段排序
    df.sort(df("age").desc, df("name").asc).show() //双字段排序
    df.select(df("name").as("username"), df("age")).show() //设置别名

    df.write.format("csv").save("file:///usr/local/spark/mycode/newpeople.csv") //保存为文件
    df.rdd.saveAsTextFile("file:///usr/local/spark/mycode/newpeople.txt") //保存为文件
    df.write.parquet("file:///usr/local/spark/mycode/newpeople.parquet") //存为列式文件


    //示例二：RDD 转DataFrame
    case class Person(name: String, age: Long) //定义一个case class

    import spark.implicits._
    val peopleRDD = spark.sparkContext.textFile("file:///opt/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt))

    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))
    val peDF = spark.sqlContext.createDataFrame(peopleRDD, Person.getClass)
    val peDF2 = spark.createDataFrame(peopleRDD, Person.getClass)
    peDF.show()
    peDF.createOrReplaceTempView("people") //必须注册为临时表才能供下面的查询使用
    val personsRDD1 = spark.sql("select name,age from people where age > 20")
    personsRDD1.map(t => "Name:" + t(0) + "," + "Age:" + t(1)).show()
  }
}