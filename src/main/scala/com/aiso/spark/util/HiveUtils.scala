package com.aiso.spark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

object HiveUtils {

  case class Person(name: String, col1: Int, col2: String)

  def saveAsTable(sc: SparkContext, filePath: String, tableName: String) {

    //    insertInto函数是向表中写入数据，可以看出此函数不能指定数据库和分区等信息，不可以直接进行写入。
    //    向hive数据仓库写入数据必须指定数据库，hive数据表建立可以在hive上建立，或者使用hiveContext.sql（“create table ...."）

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use hr")
    val data = sc.textFile(filePath).map(x => x.split("\\s+"))
      .map(x => Person(x(0), x(1).toInt, x(2)))
    data.toDF().write.mode(SaveMode.Append).insertInto(tableName)
  }


  def insert2tableByTempTable(sc: SparkContext, filePath: String, tableName: String) {

    //    insertInto函数是向表中写入数据，可以看出此函数不能指定数据库和分区等信息，不可以直接进行写入。
    //    向hive数据仓库写入数据必须指定数据库，hive数据表建立可以在hive上建立，或者使用hiveContext.sql（“create table ...."）

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use hr")
    //DataFrame数据写入hive指定数据表的分区中时产生大量小文件该怎么解决
    //    hiveContext.setConf("spark.sql.shuffle.partitions", "1")

    val data = sc.textFile(filePath).map(x => x.split("\\s+"))
      .map(x => Person(x(0), x(1).toInt, x(2)))
    data.toDF().registerTempTable("table1")

    hiveContext.sql("insert into " + tableName + " partition(date='2015-04-02') select name,col1,col2 from table1")
  }


  def load2tableByHdfsFile(sc: SparkContext, filePath: String, output_tmp_dir: String, tableName: String, date: String):
  Unit = {
    //output_tmp_dir = "/user/hdfs/rsync/tmp/apk"

    val hiveContext = new HiveContext(sc)

    hiveContext.sql("use hr")
    sc.textFile(filePath)
      .map { r => r.mkString("\001") }.repartition(100).saveAsTextFile(output_tmp_dir)

    hiveContext.sql(s"""load data inpath '$output_tmp_dir' overwrite into table $tableName partition (dt='$date')""")


  }


  //    dataframe.registerTempTable("result")
  //    sql(s"""INSERT OVERWRITE Table $outputTable PARTITION (dt ='$outputDate') select * from result""")
  //    而整个结果数据的产生只需要4分钟左右的时间，比如以下方式：将结果以textfile存入hdfs：
  //    result.rdd.saveAsTextFile(output_tmp_dir)
  //    由此可见，对hive的写入操作耗用了大量的时间。


  //    对此现象的优化可以是，将文件存为符合hive table文件的格式，然后使用hive load将产生的结果文件直接move到指定目录下。代码如下：
  //    result.rdd.map { r => r.mkString("\001") }.repartition(partitions).saveAsTextFile(output_tmp_dir)
  //    sql(s"""load data inpath '$output_tmp_dir' overwrite into table $output partition (dt='$dt')""")


}
