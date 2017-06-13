package com.aiso.spark.util

import java.util.regex.Pattern

/**
  * @author zhangyongtian
  * @define 验证工具类
  */
object ValidateUtils {



  /**
    * 判断是否是数字
    * @param s
    * @return
    */
  def isNumber(s: String) = {
    val pattern = """^(\d+)$""".r
    s match {
      case pattern(_*) => true
      case _ => false
    }
  }

  /**
    * 是否包含乱码
    * @param str
    * @return
    */
  def isContainsMessyCode(str: String): Boolean = {

    //    汉字：[0x4e00,0x9fa5]（或十进制[19968,40869]）
    //    数字：[0x30,0x39]（或十进制[48, 57]）
    //    小写字母：[0x61,0x7a]（或十进制[97, 122]）
    //    大写字母：[0x41,0x5a]（或十进制[65, 90]）
    val res = str.replaceAll("[\u4e00-\u9fa5]", "").replaceAll("\\d|\\w", "")
    println("res:" + res)
    !res.isEmpty
  }


  /**
    * 判断是否包含中文
    * @param str
    * @return
    */
  def isContainsCN(str: String): Boolean = {
    val p = Pattern.compile("[\u4e00-\u9fa5]")
    val m = p.matcher(str)

    m.find()
  }


  /**
    * 判断是否包含特殊字符
    * @param str
    * @return
    */
  def isContainsSpeciChar(str: String): Boolean = {
    val regEx = "[`~!@#$%^&*()+=|{}':;',\\\\[\\\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
    val p = Pattern.compile(regEx);
    val m = p.matcher(str);

    m.find()
  }

  /**
    * 判断是否包含指定的关键词
    * @param str
    * @param keywordArr
    * @return
    */
  def isContainsSpecWords(str: String, keywordArr: Array[String]): Boolean = {
    var result = false
    for (ele <- keywordArr if !result) {
      result = str.contains(ele)
    }
    result
  }


  def regxpTest(str: String): Boolean = {
    val regex = """(\d{8})(.+)[[ ]*|_*|\d|-第*集|第\d集|(第\d集)|大结局|先导集]{1}""".r
    println(!regex.findFirstMatchIn(str).isEmpty)
    !regex.findAllIn(str).isEmpty
  }

  def getLoggerInfo(str: String, key: String, ignore: Boolean): String = {
    var igStr = ""
    if (ignore) {
      igStr = "(?i)"
    }
    val regex = igStr + "[\\s\\S]*[<\\[]\\s*" + key + "\\s*[>\\:\\]]\\s*(\\-\\s*\\[)?\\s*([^\\[\\]<]*)[\\s<\\]]+[\\s\\S]*"
    println("regex:" + regex)
    str.replaceAll(regex, "$2")

  }


  def main(args: Array[String]): Unit = {
    println(isNumber("0.1"))

//    var res = true
//    res = isContainsMessyCode("??????asdfasdf")

    //    res = isContainsMessyCode("呵呵呵呵呵呵")
    //    Array([2020,2020,动画片,4, ,少儿, ], [蜜蜜和丽莎的魔幻旅程,蜜蜜和丽莎的魔幻旅程,动画片,21670, ,少儿, ], [『剧集』蜜蜜与莉莎的魔幻旅程,蜜蜜与莉莎的魔幻旅程,动画片,21671,2015,少儿,欧美], [“做张贺卡送祖国”fun秀进校园,“做张贺卡送祖国”fun秀进校园,动画片,23, ,少儿, ], [面包超人 咪嘉与魔法灯,面包超人：咪嘉与魔法灯,动画片,21694, ,少儿, ], [『剧集』面粉镇的节日,面粉镇的节日,动画片,21706,2008,少儿,中国大陆], [1001个玩意儿,1001个玩意儿,动画片,42, ,少儿, ], [面具熊,面具熊,动画片,21709, ,少儿, ], [面具战士,面具战士,动画片,21710, ,少儿,中国大陆], [『剧集』面具战士,面具战士,动画片,21710,2014,少儿,中国大陆], [喵星人V5动作戏,喵星人V5动作戏,动画片,21717, ,少儿, ], [喵星人的那些小破事儿,喵星人的那些小破事儿,动画片,21718, ,少儿, ], [妙趣森林,妙趣森林,动画片,21744, ,少儿, ], [妙音动漫系列,妙音动漫系列,动画片,21755, ,少儿, ], [『剧集』1到2岁绘本故事,1到2岁绘本故事,动画片,114,2013,少儿,中国大陆], [『剧集』1至2岁宝宝好习惯,1至2岁宝宝好习惯,动画片,115,2013,少儿,中国大陆], [『剧集』名画神剪历险记,名画神剪历险记,动画片,21783,2013,少儿,中国大陆], [名人爆料童年趣事,名人爆料童年趣事,动画片,21797, ,少儿, ]


//    println(res)
    //    regxpTest("12341234哈哈")
    //    extractByBookMark("asdfasdfasdfasdf嘿嘿《哈哈asdfasdf》呵呵")

    //    println(extractVideoPartOFCooCaa("2005托马斯和他的朋友们_第3季_Ⅰ集"))
    //    println("2005托马斯和他的朋友们_第3季_三集集版".replaceAll("集版", ""))
    //    println(filterTitle("2005托马斯和他的朋友们_第3季_三集合集版"))

    //    val str =
    //      "[INFO]-[20:00:00.791] RequestBuilder4cupd:160 [loggerNo: 20150720195316712874<?xml version='1.0' encoding='gbk'?><trans><send_header><tran_code>011232</tran_code><tran_date>2015-07-20</tran_date><tran_time>200000</tran_time><code>011232</code><rcv_code>000000</rcv_code>";
    //    val info = getLoggerInfo(str, "INFO", false);
    //    println(info);
    //    val loggerNo = getLoggerInfo(str, "loggerNo", false);
    //    println(loggerNo);
    //    val tran_code = getLoggerInfo(str, "tran_code", false);
    //    println(tran_code);
    //    val code = getLoggerInfo(str, "code", false);
    //    println(code);
    //    val rcv_code = getLoggerInfo(str, "rcv_code", false);
    //    println(rcv_code);
  }


}
