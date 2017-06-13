package com.aiso.spark.util

import java.text.SimpleDateFormat

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

object TimeUtils {


  final val ONE_HOUR_MILLISECONDS = 60 * 60 * 1000

  final val SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  final val DAY_DATE_FORMAT_ONE = "yyyy-MM-dd"

  final val DAY_DATE_FORMAT_TWO = "yyyyMMdd"

  //时间字符串=>时间戳
  def convertDateStr2TimeStamp(dateStr: String, pattern: String): Long = {
    new SimpleDateFormat(pattern).parse(dateStr).getTime
  }


  //时间字符串+天数=>时间戳
  def dateStrAddDays2TimeStamp(dateStr: String, pattern: String, days: Int): Long = {
    convertDateStr2Date(dateStr, pattern).plusDays(days).date.getTime
  }


  //时间字符串=>日期
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }


  //时间戳=>日期
  def convertTimeStamp2Date(timestamp: Long): DateTime = {
    new DateTime(timestamp)
  }

  //时间戳=>字符串
  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

  //时间戳=>小时数
  def convertTimeStamp2Hour(timestamp: Long): Long = {
    new DateTime(timestamp).hourOfDay().getAsString().toLong
  }


  //时间戳=>分钟数
  def convertTimeStamp2Minute(timestamp: Long): Long = {
    new DateTime(timestamp).minuteOfHour().getAsString().toLong
  }

  //时间戳=>秒数
  def convertTimeStamp2Sec(timestamp: Long): Long = {
    new DateTime(timestamp).secondOfMinute().getAsString.toLong
  }

  //判断是否是周末


  //TODO 对开始和结束时间分时(一天之内)
  //other_info最后加上"\t"
  def splitTimeByHour(other_info: String, launchTimeStamp: Long, exitTimeStamp: Long): ArrayBuffer[String] = {

    val arr = ArrayBuffer[String]()

    val launchHour = TimeUtils.convertTimeStamp2Hour(launchTimeStamp)

    val exitHour = TimeUtils.convertTimeStamp2Hour(exitTimeStamp)

    val diffTimeStamp = (exitTimeStamp - launchTimeStamp)

    if (exitHour - launchHour == 0) {
      //其他信息 整点 次数 时长
      arr.append(other_info + launchHour + "\t" + 1 + "\t" + diffTimeStamp / 1000)
    }

    if (exitHour - launchHour > 0) {
      val launchHour = TimeUtils.convertTimeStamp2Hour(launchTimeStamp)
      val launchMinute = TimeUtils.convertTimeStamp2Minute(launchTimeStamp)
      val launchSec = TimeUtils.convertTimeStamp2Sec(launchTimeStamp)

      val exitHour = TimeUtils.convertTimeStamp2Hour(exitTimeStamp)
      val exitMinute = TimeUtils.convertTimeStamp2Minute(exitTimeStamp)
      val exitSec = TimeUtils.convertTimeStamp2Sec(exitTimeStamp)

      arr.append(other_info + launchHour + "\t" + 1 + "\t" + (60 * 60 - (launchMinute * 60 + launchSec)))

      var h = launchHour
      //需要分时
      while (h < exitHour) {
        h = h + 1
        if (h != exitHour) {
          arr.append(other_info + h +
            "\t" + 0 + "\t" + 1 * 60 * 60)
        } else {
          arr.append(other_info + h + "\t" + 0 + "\t" + (exitMinute * 60 + exitSec))
        }
      }
    }

    arr
  }

  //对开始和结束时间分时
  def splitTimeByMinute(other_info: String, startTime: String, endTime: String): ArrayBuffer[String] = {

    val arr = ArrayBuffer[String]()
    //00:01  00:29
    val startHour = startTime.charAt(0).toString.toInt == 0 match {
      case true => startTime.charAt(1).toString.toInt
      case false => startTime.substring(0, 2).toInt
    }

    //    println("startHour:" + startHour)


    val startMin = startTime.charAt(3).toString.toInt == 0 match {
      case true => startTime.charAt(4).toString.toInt
      case false => startTime.substring(3).toInt
    }

    //    println("startMin:" + startMin)


    val endHour = endTime.charAt(0).toString.toInt == 0 match {
      case true => endTime.charAt(1).toString.toInt
      case false => endTime.substring(0, 2).toInt
    }

    //    println("endHour:" + endHour)

    val endMin = endTime.charAt(3).toString.toInt == 0 match {
      case true => endTime.charAt(4).toString.toInt
      case false => endTime.substring(3).toInt
    }

    //    println("endMin:" + endMin)

    val diffHour = endHour - startHour

    if (diffHour == 0) {

      var i = startMin

      while (i <= endMin) {
        arr.append(other_info + startHour.toString + "\t" + i.toString)
        i = i + 1
      }

    }

    if (diffHour > 0) {
      var i = startMin

      while (i <= 59) {
        arr.append(other_info + startHour.toString + "\t" + i.toString)
        i = i + 1
      }


      var h = startHour
      //需要分时
      while (h < endHour) {
        h = h + 1
        if (h != endHour) {
          for (n <- 0 to 59) {
            arr.append(other_info + h.toString + "\t" + n.toString)
          }
        } else {
          for (n <- 0 to endMin) {
            arr.append(other_info + h.toString + "\t" + n.toString)
          }
        }
      }

      //      for (i <- 1 to diffHour) {
      //
      //
      //      }


    }

    arr
  }

  def addZero(hourOrMin: String): String = {
    if (hourOrMin.toInt <= 9)
      "0" + hourOrMin
    else
      hourOrMin

  }

  def delZero(hourOrMin: String): String = {
    var res = hourOrMin
    if (!hourOrMin.equals("0") && hourOrMin.startsWith("0"))
      res = res.replaceAll("^0","")
    res
  }


  def getTimeNum(time: String): Int = {
    1
  }

  def dateStrPatternOne2Two(time: String): String = {
    TimeUtils.convertTimeStamp2DateStr(TimeUtils.convertDateStr2TimeStamp(time, TimeUtils
      .DAY_DATE_FORMAT_ONE), TimeUtils.DAY_DATE_FORMAT_TWO)
  }

  def main(args: Array[String]) {

    //1477483944000
    //2016/10/26 15:15   2016/10/26 18:10
    //    val arr = splitTime("", 1477483944000L, 1477484004000L)
    //            val arr = splitTime("",1477484004000L, 1477490400000L)
    //2016/10/25 23:3:24 2016/10/26 1:13:0
    //    val arr = splitTime("", 1477411200000L, 1477415580000L)
    //    for (i <- 0 until arr.length) yield println(arr(i)) //将得到ArrayBuffer(2,6,4,-2,-4)
//    splitTimeByMinute("", "00:01", "02:29")
//    println(delZero("2109"))
    val res = None.toString

    println(addZero("1"))
  }


}
