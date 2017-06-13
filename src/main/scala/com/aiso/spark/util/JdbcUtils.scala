package com.aiso.spark.util

import java.sql.DriverManager

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


object JdbcUtils {

  //TODO 读取Mysql表=>DataFrame
  def readMysql2DF(sc: SparkContext, host: String, prop: java.util.Properties, db: String, tableName: String): DataFrame
  = {

    val sqlContext = new SQLContext(sc)
    val url: String = "jdbc:mysql://" + host + ":3306/" + db +
      "?useUnicode=true&characterEncoding=utf-8&useSSL=false"

    sqlContext.read.jdbc(url, tableName, prop)
  }


  //TODO 通过jdbc 将DataFrame写入mysql
  def writeDF2Mysql(sc: SparkContext, df: DataFrame,
                    prop: java.util.Properties, db: String, tableName: String, isTruncate: Boolean, saveMode: SaveMode) = {

    //    val url: String = "jdbc:mysql://" + sc.getConf.get("mysql.host") + ":3306/" + sc.getConf.get("mysql.db") + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"

    val url: String = "jdbc:mysql://" + sc.getConf.get("mysql.host") + ":3306/" + db +
      "?useUnicode=true&characterEncoding=utf-8&useSSL=false"

    //清空数据
    //    if (isTruncate)
    truncate(url, tableName, prop)

    df.write
      .mode(saveMode)
      .jdbc(url, tableName, prop)

  }

  //TODO 清空表数据
  def truncate(url: String, tableName: String, prop: java.util.Properties) = {
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(url + "&user=" + prop.getProperty("user") + "&password=" + prop.getProperty("password"))
    try {
      val statement = conn.createStatement()
      statement.executeUpdate("TRUNCATE " + tableName);
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    finally {
      conn.close
    }
  }


  //TODO 通过JDBC操作Hive
  def hiveJdbc() {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://hadoop2:10000/hive", "hadoop", "")
    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery("select ordernumber,amount from tbStockDetail  where amount>3000")
      while (rs.next) {
        val ordernumber = rs.getString("ordernumber")
        val amount = rs.getString("amount")
        println("ordernumber = %s, amount = %s".format(ordernumber, amount))
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    conn.close
  }


  //TODO Main
  def main(args: Array[String]) {
    /*  val config: Config = ConfigFactory.load()
      val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("test")
      val sc = new SparkContext(conf)
      val prop = new java.util.Properties()
      prop.put("user", config.getString("mysql.user"))
      prop.put("password", config.getString("mysql.password"))
      prop.put("driver", "com.mysql.jdbc.Driver")
      val tbDF = readMysql2DF(sc, config.getString("mysql.host"), prop, "vboxDB", "SYS_SHOW_USER")
      tbDF.foreach(print(_))
      sc.stop()*/
  }

}
