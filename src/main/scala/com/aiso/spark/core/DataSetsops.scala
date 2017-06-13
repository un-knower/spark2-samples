package com.aiso.spark.core

import org.apache.spark.sql.SparkSession

object DataSetsops {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DatasetOps")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///G:/IMFBigDataSpark2016/IMFScalaWorkspace_spark200/Spark200/spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val personDF = spark.read.json("G:\\IMFBigDataSpark2016\\spark-2.0.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
    // personDF.show()
    // personDF.collect().foreach (println)
    // println(personDF.count())

    val personDS = personDF.as[Person]
    // personDS.show()
    // personDS.printSchema()
    //val dataframe=personDS.toDF()

    personDF.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age > 20").show()
    spark.sql("select * from persons where age > 20").explain()

    val personScoresDF = spark.read.json("G:\\IMFBigDataSpark2016\\spark-2.0.0-bin-hadoop2.6\\examples\\src\\main\\resources\\peopleScores.json")
    // personDF.join(personScoresDF,$"name"===$"n").show()
    personDF.filter("age > 20").join(personScoresDF, $"name" === $"n").show()

    personDF.filter("age > 20")
      .join(personScoresDF, $"name" === $"n")
      .groupBy(personDF("name"))
      .agg(avg(personScoresDF("score")), avg(personDF("age")))
      .explain()
    //.show()

    while (true) {}

    spark.stop()
  }
}
