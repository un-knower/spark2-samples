package com.aiso.spark.util

import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * HBase工具类
  */
object HBaseUtils {


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

    myConf.set("hbase.regionserver.thread.compaction.large", "5")

    myConf.set("hbase.regionserver.thread.compaction.small", "5")

    //    myConf.set("hbase.hregion.majorcompaction","0")

    myConf.set("hbase.hstore.compaction.min", "10")

    myConf.set("hbase.hstore.compaction.max", "10")

    myConf.set("hbase.hstore.blockingStoreFiles", "100")

    myConf.set("hbase.hstore.compactionThreshold", "7")

    myConf.set("hbase.regionserver.handler.count", "100")

    myConf.set("hbase.regionserver.hlog.splitlog.writer.threads", "10")

    myConf.set("hbase.regionserver.thread.compaction.small", "5")

    myConf.set("hbase.regionserver.thread.compaction.large", "8")

    myConf.set("hbase.hregion.max.filesizes", "4G")

    myConf.set("hbase.hregion.max.filesize", "60G")

    myConf.set("hbase.hregion.memstore.flush.size", "60G")

    myConf.set("hbase.hregion.memstore.block.multiplier", "5")

    myConf.set("hbase.hstore.compaction.min", "7")

    //    myConf.set("hbase.client.write.buffer", "8388608")

    myConf.set("hbase.client.pause", "200")

    myConf.set("hbase.client.retries.number", "71")

    myConf.set("hbase.ipc.client.tcpnodelay", "false")

    myConf.set("hbase.client.scanner.caching", "500")




    ConnectionFactory.createConnection(myConf);

  }

  def getMutator(tableName: String): BufferedMutator = {
    getHbaseConn().getBufferedMutator(TableName.valueOf(tableName))
  }


  ///////////////////////////////////////////////////特征向量清洗///////////////////////////////////////////////////////////////

  /**
    * 获取UserVectorTerminal表的Put对象
    *
    * @param sortedLine
    * @return
    */
  def getPut_UserVectorTerminal(sortedLine: String): Put = {

    val cols = sortedLine.split('\t')

    val put = new Put(Bytes.toBytes(cols(0)))

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sn"), Bytes.toBytes(cols(0)))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("brand"), Bytes.toBytes(cols(1)))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("province"), Bytes.toBytes(cols(2)))

    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(cols(3)))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("size"), Bytes.toBytes(cols(4)))

    //    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pro_year"), Bytes.toBytes(cols(5)))

    put

  }
}
