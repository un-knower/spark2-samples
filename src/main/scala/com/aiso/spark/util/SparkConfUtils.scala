package com.aiso.spark.util

import org.apache.spark.SparkConf

object SparkConfUtils {

  def optimize(conf: SparkConf): SparkConf = {
    conf.set("spark.sql.codegen", "false");
    conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false");
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
    conf.set("spark.sql.parquet.compression.codec", "snappy");
    conf.set("spark2.default.parallelism", "100")
    conf.set("spark2.storage.memoryFraction", "0.5")
    conf.set("spark2.shuffle.consolidateFiles", "true")
    conf.set("spark2.shuffle.file.buffer", "64")
    conf.set("spark2.shuffle.memoryFraction", "0.3")
    conf.set("spark2.reducer.maxSizeInFlight", "24")
    conf.set("spark2.shuffle.io.maxRetries", "60")
    conf.set("spark2.shuffle.io.retryWait", "60")
    conf.set("spark2.serializer", "org.apache.spark2.serializer.KryoSerializer")
    //    conf.registerKryoClasses(new Class[]{
    //        ClassOne.class,
    //        ClassTwo.class});
    conf
  }


}
