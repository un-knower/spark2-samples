package com.aiso.spark.core

import org.apache.spark.sql.SparkSession

object Spark2Demo {

  def main(args: Array[String]) {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
    //get all settings
    val configMap: Map[String, String] = spark.conf.getAll

    //fetch metadata data from the catalog
    spark.catalog.listDatabases.show(false)
    spark.catalog.listTables.show(false)


    //create a Dataset using spark.range starting from 5 to 100, with increments of 5
    val numDS = spark.range(5, 100, 5)
    // reverse the order and display first 5 items
    numDS.orderBy("id").show(5)
    //    numDS.orderBy(desc("id")).show(5)
    //compute descriptive stats and display them
    numDS.describe().show()
    // create a DataFrame using spark.createDataFrame from a List or Seq
    val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
    //rename the columns
    val lpDF = langPercentDF.withColumnRenamed("_1", "language").withColumnRenamed("_2", "percent")
    //order the DataFrame in descending order of percentage
    lpDF.orderBy("percent").show(false)
    //    lpDF.orderBy(desc("percent")).show(false)


    // read the json file and create the dataframe
    val jsonFile = args(0)
    val zipsDF = spark.read.json(jsonFile)
    //filter all cities whose population > 40K
    zipsDF.filter(zipsDF.col("pop") > 40000).show(10)


    // Now create an SQL table and issue SQL queries against it without
    // using the sqlContext but through the SparkSession object.
    // Creates a temporary view of the DataFrame
    zipsDF.createOrReplaceTempView("zips_table")
    zipsDF.cache()
    val resultsDF = spark.sql("SELECT city, pop, state, zip FROM zips_table")
    resultsDF.show(10)



    //drop the table if exists to get around existing table error
    spark.sql("DROP TABLE IF EXISTS zips_hive_table")
    //save as a hive table
    spark.table("zips_table").write.saveAsTable("zips_hive_table")
    //make a similar query against the hive table
    val resultsHiveDF = spark.sql("SELECT city, pop, state, zip FROM zips_hive_table WHERE pop > 40000")
    resultsHiveDF.show(10)


  }
}
