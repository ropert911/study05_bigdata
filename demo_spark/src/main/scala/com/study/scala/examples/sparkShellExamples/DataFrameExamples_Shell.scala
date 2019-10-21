package com.study.scala.examples.sparkShellExamples

import org.apache.spark.sql.SparkSession

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
    // 打印模式信息
    df.printSchema()
    /*
    root
    |-- age: long    (nullable = true)
    |-- name: string    (nullable = true)
     */

    df.show()
    /*
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
     */


    // 选择多列
    df.select(df("name"), df("age") + 1).show()
    /*
    +-------+---------+
    | name |(age + 1) |
    +-------+---------+
    | Michael |null |
    | Andy |31 |
    | Justin |20 |
    +-------+---------+
     */

    // 条件过滤
    df.filter(df("age") > 20).show()
    /*
    +---+----+
    | age | name |
    +---+----+
    |30 | Andy |
    +---+----+
    */

    // 分组聚合
    df.groupBy("age").count().show()
    /*
    +----+-----+
    | age | count |
    +----+-----+
    |    19 | 1 |
    |    null | 1 |
    |    30 | 1 |
    +----+-----+
    */

    // 排序
    df.sort(df("age").desc).show()
    /*
    +----+-------+
    | age | name |
    +----+-------+
    |30 | Andy |
    |19 | Justin |
    |null | Michael |
    +----+-------+
    */

    //多列排序
    df.sort(df("age").desc, df("name").asc).show()
    /*
    +----+-------+
    | age | name |
    +----+-------+
    |30 | Andy |
    |19 | Justin |
    |null | Michael |
    +----+-------+
    */

    //对列进行重命名
    df.select(df("name").as("username"), df("age")).show()
    /*
    +--------+----+
    | username | age |
    +--------+----+
    | Michael |null |
    | Andy |30 |
    | Justin |19 |
    +--------+----+
    */
  }


  /**
    * Datafram缓存，技术sql等
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
    * 不同的格式
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


    //读取部分字段，以csv格式保存
    peopleDF3.select("name", "age").write.format("csv").save("file:///usr/local/spark/mycode/newpeople.csv")
    //保存为parquet格式
    peopleDF3.write.parquet("file:///usr/local/spark/mycode/newpeople.parquet")
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

  def jdbcTest(): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark"
    val jdbcDriver = "com.mysql.jdbc.Driver"

    val spark = SparkSession.builder().getOrCreate()
    val jdbcDF = spark.read.format("jdbc")
      .option("url", jdbcUrl).option("driver", jdbcDriver)
      .option("user", "root") //用户名
      .option("password", "hadoop") //密码
      .option("dbtable", "student") //加载的表
      .load()
    jdbcDF.show()

    //保存到hive中
    jdbcDF.write.mode("overwrite").saveAsTable("testwarehouse.testtt")
  }

  def hiveTest(): Unit = {
    case class Record(key: Int, value: String)

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

    import spark.sql
    //下面是运行结果
    sql("SELECT * FROM sparktest.student").show()
    /*
    +---+--------+------+---+
    | id| name|gender|age|
    +---+--------+------+---+
    | 1| Xueqian| F| 23|
    | 2|Weiliang| M| 24|
    +---+--------+------+---+
    */


    //##########写入数据
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))

    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))

    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    //查看studentDF
    studentDF.show()
    /*
    +---+---------+------+---+
    | id| name|gender|age|
    +---+---------+------+---+
    | 3|Rongcheng| M| 26|
    | 4| Guanhua| M| 27|
    +---+---------+------+---+
    */
    //下面注册临时表
    studentDF.createOrReplaceTempView("tempTable")

    sql("insert into sparktest.student select * from tempTable")

  }
}
