package com.aiso.spark.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object HBaseBulkLoader {

  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("coocaa-ApkDataLoadJob")
  val sc = new SparkContext(sparkConf)

  //  val sparkConf = new SparkConf().
  //    setMaster("spark://lxw1234.com:7077").
  //    setAppName("lxw1234.com")

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "")

  def hfile2Table(hfilePath: String, outputTable: String): Unit = {
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    val table = new HTable(hbaseConf, outputTable)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path(hfilePath), table)

    sc.stop()

  }


  def insert(hdfsFilePath: String, hfilePath: String, family: String, col: String, outputTable: String): Unit = {

    val domainUid = sc.textFile(hdfsFilePath).map { x =>
      val a = x.split(",")
      val domain = a(0)
      val uid = a(1)
      (domain, uid)
    }
    val result = domainUid.distinct().sortByKey(numPartitions = 1).map { x =>
      val domain = x._1
      val uid = x._2
      val kv: KeyValue = new KeyValue(Bytes.toBytes(domain), family.getBytes(), col.getBytes(), uid.getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(domain)), kv)
    }

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    result.saveAsNewAPIHadoopFile(hfilePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf)

    sc.stop()
  }


  def insert2(hdfsFilePath: String, hfilePath: String, family: String, col: String, outputTable: String): Unit = {

    var rdd1 = sc.makeRDD(Array(("A", 2), ("B", 6), ("C", 7)))
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ", "zkNode1,zkNode2,zkNode3")
    sc.hadoopConfiguration.set("zookeeper.znode.parent", "/hbase")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, outputTable)

    var job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    rdd1.map(
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable, put)
      }
    ).saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }


}
