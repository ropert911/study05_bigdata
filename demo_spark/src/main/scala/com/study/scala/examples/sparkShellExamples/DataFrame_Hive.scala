package com.study.scala.examples.sparkShellExamples

import org.apache.spark.sql.SparkSession

/**
  * @author sk-qianxiao
  * @date 2019/10/21
  */
object DataFrame_Hive {
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


    //保存到hive中
    studentDF.write.mode("overwrite").saveAsTable("testwarehouse.testtt")
  }
}
