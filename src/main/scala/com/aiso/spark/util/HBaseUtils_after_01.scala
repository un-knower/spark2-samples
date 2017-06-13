package com.aiso.spark.util

import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * HBase工具类
  */
object HBaseUtils_after_01 {

  /**
    * 获取Hbase连接
    *
    * @return
    */
  def getHbaseConn(): Connection = {

    //      val myConf = HBaseConfiguration.create()
    //      myConf.set("hbase.zookeeper.quorum", "192.168.1.11,192.168.1.12,192.168.1.13")
    //      myConf.set("hbase.zookeeper.property.clientPort", "2181")
    //      val hbaseConn = ConnectionFactory.createConnection(myConf);
    //      val mutator = hbaseConn.getBufferedMutator(TableName.valueOf("tableName"))


    val myConf = HBaseConfiguration.create()

    //    val zookeeper_quorum = sc.getConf.get("hbase.zookeeper.quorum")
    //    myConf.set("hbase.zookeeper.quorum", "192.168.79.131")

    myConf.set("hbase.zookeeper.quorum", "192.168.1.11,192.168.1.12,192.168.1.13")

    myConf.set("hbase.zookeeper.property.clientPort", "2181")

    ConnectionFactory.createConnection(myConf);

  }

  def getMutator(tableName: String): BufferedMutator = {
    getHbaseConn().getBufferedMutator(TableName.valueOf(tableName))
  }


  ///////////////////////////////////////////////////数据清洗///////////////////////////////////////////////////////////////

  /**
    * 获取terminal表的Put对象
    *
    * @param brand 厂家
    * @param sortedLine
    * @return
    */
  def getPut_terminal(brand: String, sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    val put = new Put(Bytes.toBytes(cols(6) + brand))

    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("license"), Bytes.toBytes(cols(0)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("province"), Bytes.toBytes(cols(1)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("last_poweron"), Bytes.toBytes(cols(2)))

    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("model"), Bytes.toBytes(cols(3)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("size"), Bytes.toBytes(cols(4)))

    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("city"), Bytes.toBytes(cols(5)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("brand"), Bytes.toBytes(brand))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("sn"), Bytes.toBytes(cols(6)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("area"), Bytes.toBytes(cols(7)))
    put.addColumn(Bytes.toBytes("terminalProperty"), Bytes.toBytes("citylevel"), Bytes.toBytes(cols(8)))

    put

  }

  /**
    * 获取EPG表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_epg(sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    val put = new Put(Bytes.toBytes(cols(1) + "-" + cols(2) + "-" + cols(0) + "-" + cols(6) + "-" + cols(7)))

    var i = 0
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("channel"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("tv_date"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("start_end"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("time_range"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("pg_type"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("pg"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("tv_hour"), Bytes.toBytes(cols(i)))

    i = i + 1
    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("tv_min"), Bytes.toBytes(cols(i)))

    put

  }

  /**
    * 获取apk表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_apk(sortedLine: String): Put = {

    //    val dimAreaCol = Bytes.toBytes("dim_area")

    val cols = sortedLine.split('\t')

    //    val put = new Put(Bytes.toBytes(cols(1) + "-" + cols(2) + "-" + cols(0) + "-" + cols(6) + "-" + cols(7)))

    //    val result = sn + "\t" + apkPackage + "\t" + date + "\t" + hour + "\t" + launchCnt + "\t" + duration


    val put = new Put(Bytes.toBytes(cols(0) + cols(1) + cols(2) + cols(3) + "CC"))

    var i = 0
    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_apk"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_hour"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_duration"), Bytes.toBytes(cols(i)))

    put

  }

  def getPut_apkGQ(sortedLine: String): Put = {

    //    val dimAreaCol = Bytes.toBytes("dim_area")

    val cols = sortedLine.split('\t')

    //    val put = new Put(Bytes.toBytes(cols(1) + "-" + cols(2) + "-" + cols(0) + "-" + cols(6) + "-" + cols(7)))

    //    val result = sn + "\t" + apkPackage + "\t" + date + "\t" + hour + "\t" + launchCnt + "\t" + duration


    val put = new Put(Bytes.toBytes(cols(0) + cols(1) + cols(2) + cols(3) + "CC"))

    var i = 0
    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_apk"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_hour"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_duration"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("logtime"), Bytes.toBytes(cols(i)))

    put

  }


  /**
    * 龚琴推总测试表
    * sn,
    * apkPackage,
    * date,
    * SUM(duration) dura,
    * SUM(launchCnt) launchCnts
    *
    * @param sortedLine
    * @return
    */
  def getPut_apk_gq(sortedLine: String): Put = {

    //    val dimAreaCol = Bytes.toBytes("dim_area")

    val cols = sortedLine.split('\t')

    //    val put = new Put(Bytes.toBytes(cols(1) + "-" + cols(2) + "-" + cols(0) + "-" + cols(6) + "-" + cols(7)))

    //    val result = sn + "\t" + apkPackage + "\t" + date + "\t" + hour + "\t" + launchCnt + "\t" + duration


    val put = new Put(Bytes.toBytes(cols(0) + cols(1) + cols(2) + cols(3) + "CC"))

    var i = 0
    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_apk"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("this_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_duration"), Bytes.toBytes(cols(i)))


    put

  }

  //获取到剧表的Put对象
  def getPut_Plays(sortedLine: String): Put = {

    //    val dimAreaCol = Bytes.toBytes("dim_area")

    val cols = sortedLine.split('\t')

    //    val put = new Put(Bytes.toBytes(cols(1) + "-" + cols(2) + "-" + cols(0) + "-" + cols(6) + "-" + cols(7)))

    // vid：title 剧名-集数
    val put = new Put(Bytes.toBytes(cols(0) + cols(3) + cols(4) + cols(5) + "CC"))

    var i = 0
    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_title"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_awcid"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_part"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("dim"), Bytes.toBytes("dim_hour"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_vv"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("fact_duration"), Bytes.toBytes(cols(i)))
    i = i + 1

    put

  }


  ///////////////////////////////////////推总/////////////////////////////////////////////////////////

  /**
    * 获取开关机 推总表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_total_oc(sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    var i = 0

    val put = new Put(Bytes.toBytes(cols(0)+cols(1)+cols(2)))

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("power_on_day"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("power_on_time"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("power_on_length"), Bytes.toBytes(cols(i)))

    put

  }


  /**
    * 获取直播 推总表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_total_live(sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    var i = 0

    val put = new Put(Bytes.toBytes(cols(0)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_channel"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_hour"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_min"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fact_cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fact_time_length"), Bytes.toBytes(cols(i)))


    put

  }


  /**
    * 获取APK 推总表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_total_apk(sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    val put = new Put(Bytes.toBytes(cols(0) + cols(1) + cols(2) + cols(3) + "CC"))

    var i = 0

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_sn"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_apk"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_date"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dim_hour"), Bytes.toBytes(cols(i)))
    i = i + 1


    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fact_cnt"), Bytes.toBytes(cols(i)))
    i = i + 1

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fact_duration"), Bytes.toBytes(cols(i)))

    put

  }

}
