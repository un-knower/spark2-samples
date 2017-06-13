package com.aiso.spark.streaming

import java.io.FileOutputStream

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream.fromReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkStreaming里面使用文本分析模型（2.0.1）
标签： sparkStrea文本分析模型贝叶斯
2017-01-16 11:19 299人阅读 评论(0) 收藏 举报
 分类： spark（15）
版权声明：欢迎进群一起探讨 企鹅群246068961
如果使用模型的建立话请参考另一篇博客建模地址
功能：接收来自kafka的数据，数据是一篇文章，来判断文章的类型，把判断的结果一并保存到Hbase，并把文章建立索引（没有代码只有一个空壳，可以自己实现，以后有机会了可能会补上），
代码实现：


  http://blog.csdn.net/hanlipenghanlipeng/article/details/54232965

  */
object UseModel {
  //流程代码
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads) = Array("192.168.10.199:2181", "order", "order", "2");
    val conf = new SparkConf().setAppName("useModel").setMaster("local[4]");
    val ssc = getStreamingContext(conf, 10);
    val dstreams = getKafkaDstream(ssc, topics, zkQuorum, group, numThreads);
    val dstream = dstreams.inputDStream.map(_._2);
    dstream.persist()
    //测试
    dstream.print()
    //如果能判断不为空就更好了
    dstream.foreachRDD(rdd => everyRDD(rdd))
    ssc.start()
    ssc.awaitTermination()
  }


  //得到StreamingContext
  def getStreamingContext(conf: SparkConf, secend: Int): StreamingContext = {
    return new StreamingContext(conf, Seconds(secend))
  }

  //得到sparkSession
  def getSparkSession(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "warehouse/dir")
      .getOrCreate()
    return spark;
  }

  //得到kafkaDStream
  def getKafkaDstream(ssc: StreamingContext, topics: String, zkQuorum: String, group: String, numThreads: String): JavaPairReceiverInputDStream[String, String] = {
    ssc.checkpoint("directory")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap;
    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    return stream;
  }

  //文件保存测试
  def savaString(str: String): Unit = {
    val out = new FileOutputStream("D:\\decstop\\file.txt", true);
    out.write(str.getBytes)
    out.flush()
    out.close()
  }

  //每一个rdd做动作
  def everyRDD(rdd: RDD[String]) {
    val sameModel = NaiveBayesModel.load("resoult")

    val spark = getSparkSession(rdd.context.getConf)
    val rddDF = rdd.map { line => (1, line) }.toDF("label", "text").persist()
    //rddDF.show()
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val tokenizerRDD = tokenizer.transform(rddDF)
    //tokenizerRDD.show(false)

    val hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    val hashingTFRDD = hashingTF.transform(tokenizerRDD)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(hashingTFRDD)
    val rescaledData = idfModel.transform(hashingTFRDD)
    //rescaledData.show(false)
    //转化为贝叶斯需要的格式
    val useDataRdd = rescaledData.select($"label", $"features").map {
      case Row(label: Int, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    val predictions = sameModel.transform(useDataRdd)
    predictions.persist()
    //predictions.show(false)
    //参照下面可以实现各种的逻辑，可以把下面的保存，建索引都加上
    predictions.select($"label", $"prediction").foreach { x => savaString(("" + x.getAs("label") + " " + x.getAs("prediction") + "\n\r")) }

    //测试
    predictions.createOrReplaceTempView("prediction")
    rddDF.createOrReplaceTempView("atical")

    //spark.sql("select p.label,p.prediction,a.text from prediction p,atical a where p.label=a.label").select(col, cols)


  }

  //简历索引 主要的建立索引的有hbase_rowKay(time) aothor title article
  def buiderIndex() {}

  //保存到hbase
  def savaToHbase() {

  }

  //发送到下一个kafka 发送的数据 time 正舆情数量 负面舆情数量 百分比 是否报警

  def sendToKafka() {

  }


}
