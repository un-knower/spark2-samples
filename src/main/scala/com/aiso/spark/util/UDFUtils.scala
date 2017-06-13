package com.aiso.spark.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext


object UDFUtils {

  def main(args: Array[String]) {
    println(dayOfWeek("2017-05-14"))
  }


  def registerUDF(sqlContext: SQLContext, udfName: String): Unit = {
    udfName match {
      case "dayOfWeek" => sqlContext.udf.register(udfName, dayOfWeek _)
    }

  }


  def dayOfWeek(dateStr: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(dateStr)

    //    val sdf2 = new SimpleDateFormat("EEEE")
    //    sdf2.format(date)

    val cal = Calendar.getInstance();
    cal.setTime(date);
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1;

    //星期天 默认为0
    if (w <= 0)
      w = 7
    w
  }


}