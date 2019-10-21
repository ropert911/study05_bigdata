package com.study.scala.examples.sparkShellExamples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 这里的示例大部分有运行过，主要是参考
  *
  * @author sk-qianxiao
  * @date 2019/10/18
  */
object DataFrameExamples_Shell {

  //只有吧定义放到方法外，toDF才能编译生效
  case class Person(name: String, age: Long)

  def test1(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    //读取json格式数据
    val df = spark.read.json("file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json")
    df.printSchema() //打印表结构信息
    df.show() //全部显示
    df.select(df("name"), df("age") + 1).show() // 选择多列
    df.filter(df("age") > 20).show() // 条件过滤
    df.groupBy("age").count().show() // 分组聚合
    df.distinct().count() //去重操作
    df.sort(df("age").desc).show() // 排序
    df.sort(df("age").desc, df("name").asc).show() //多列排序
    df.select(df("name").as("username"), df("age")).show() //对列进行重命名
  }


  /**
    * Datafram 缓存，spark sql等
    */
  def test2(): Unit = {
    val filepath = "file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.txt"
    val spark = SparkSession.builder().getOrCreate()

    //导入包，支持把一个RDD隐式转换为一个DataFrame,要不toDF要失败
    import spark.implicits._
    val peopleDF1 = spark.sparkContext.textFile(filepath).map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()
    val peopleDF = spark.read.format("json").load("file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json")
    //DataFrame的监听视图，方便做sql操作
    peopleDF.createOrReplaceTempView("people")
    //这里不做持久化也只是，只是为了方便多次action，一般都要挂念化
    spark.sqlContext.cacheTable("people")

    val personsRDD = spark.sql("select name,age from people where age > 20")
    //DataFrame中的每个元素都是一行记录，包含name和age两个字段，分别用t(0)和t(1)来获取值
    personsRDD.map(t => "Name:" + t(0) + "," + "Age:" + t(1)).show()

    spark.sqlContext.uncacheTable("people")
  }


  /**
    * RDD 转为 DataFrame, 通过结构Persion进行转换
    */
  def rddToDataFrame() {
    val spark = SparkSession.builder().getOrCreate()

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

  /**
    * 通过定义 StructType 进行转换
    */
  def rddToDataFrame2() {
    case class Record(key: Int, value: String)

    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    spark.sql("SELECT * FROM sparktest.student").show()

    val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    val studentDF = spark.createDataFrame(rowRDD, schema)
  }
}
