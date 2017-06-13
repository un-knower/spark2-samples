package com.aiso.spark.util

import java.util.regex.Pattern

import org.apache.spark.sql.Row

/**
  * @author zhangyongtian
  * @define 验证工具类
  */
object ValidateUtils03 {

  /**
    * 酷开到剧剧名清洗
    *
    * @param log_dim_title
    * @return
    */
  def extractVideoNameOFCooCaa(log_dim_title: String, filmInfoArr: Array[Row]): String = {

    val versionArr = Array[String](
      "未删减版", "[未删减版]", "完整版", "全集", "合集", "完全版", "[TV版]", "精华版", "国语", "（国语）", "（国语版）", "国语版", "国语中字", "（英语版）", "英语中字", "（英语）", "[英语版]", "英语版", "[英语]", "粤语版", "粤语", "（粤语版）", "（粤语）", "[粤语版]", "[粤语]", "日语版", "（日语版）", "日语", "中文版", "TV中文版", "（中文版）", "韩语版", "[韩语版]", "韩语中字", "四川话版", "云南话版", "东北话版", "天津话版", "兰州话版", "潮汕话版", "陕西话版", "闽南语版", "上海话版", "日配版", "法语版", "卫视版", "央视版", "TVB版", "浙江卫视版", "湖南卫视版", "东方卫视版", "安徽卫视版", "深圳卫视版", "旅游卫视版", "江西卫视版", "DVD版", "网络版", "电视版", "版权版", "OVA", "标准版", "原版", "未删剪原版", "4K版", "（4K）", "VR版", "（VR）", "3D版", "【3D版】", "（新3D版）", "（3D）", "3D", "标清版", "_标清", "蓝光真高清", "（蓝光真高清）", "（清晰版）", "高清版", "高清字幕版", "【高清】", "[高清版]", "春节贺岁版", "纯享版", "精简版", "加长版", "（加长版）", "（加长重映版）", "精编版", "重制版", "字幕版", "双语字幕版", "（双语字幕版）", "高清无字幕版", "完整字幕版", "中英字幕版", "免费版", "（免费版）", "[免费版]", "（原声）", "原声", "原声高清版", "英文原声高清版", "特别版", "生肖特别版", "圣诞特别版", "完全版"
    )

    //TODO 判断 module
    //从书名号中提取 数字 特殊符号清除 [空 国语版]
    //通过 ****版本 判断是电影

    filmInfoArr.foreach(row => {

      //original_name,model,id,year,crowd,region
      val original_name = isNullorEmptyHandle(row.getString(0))
      val standard_name = isNullorEmptyHandle(row.getString(1))
      val module = isNullorEmptyHandle(row.getString(2))
      val id = isNullorEmptyHandle(row.getString(3))
      val year = isNullorEmptyHandle(row.getString(4))
      val crowd = isNullorEmptyHandle(row.getString(5))
      val region = isNullorEmptyHandle(row.getString(6))


      //去掉版本匹配
      var delVersionTitle = log_dim_title
      for (version <- versionArr) {
        if (log_dim_title.contains(version)) {
          delVersionTitle = log_dim_title.substring(0, log_dim_title.indexOf(version))
        }
      }

      //提取书名号中的内容
      val delBookMarkTitle = extractByBookMark(log_dim_title)

      //数字转换匹配
      var changeNumTitle = log_dim_title
      val luomaNumMap = Constant.luomaNumMap
      luomaNumMap.keys.foreach(i =>
        changeNumTitle = changeNumTitle.replaceAll(i, luomaNumMap.get(i).get)
      )

      val zhNumMap = Constant.zhNumMap
      zhNumMap.keys.foreach(i =>
        changeNumTitle = changeNumTitle.replaceAll(i, zhNumMap.get(i).get)
      )

      //去特殊符号匹配
      var log_dim_title_spec = log_dim_title.trim
      log_dim_title_spec = log_dim_title.replaceAll(":|,|!|。|：|，|！|•   |.", "")

      //去剧集匹配
      var delPartTitle = log_dim_title.trim

      if(delPartTitle.contains("第")){
        delPartTitle= delPartTitle.trim.substring(0,delPartTitle.indexOf("第")).trim
      }

      if(log_dim_title.contains("_")){
        delPartTitle= delPartTitle.trim.substring(0,delPartTitle.indexOf("_")).trim
      }

      val titleArr = Array[String](log_dim_title, delVersionTitle, delBookMarkTitle, changeNumTitle,
        log_dim_title_spec,delPartTitle)


      ///////////////////////////////////////////////////////////////////////////
      //电影 【电影名称】【版本】
      if(module.equals("电影")){
        for (title <- titleArr) {
          if (title.equals(original_name)) {
            return standard_name + "\t" + module + "\t" + id + "\t" + year + "\t" + crowd + "\t" + region
          }
        }
      }


      /////////////////////////////////////////////////////////////////////////////////
      //电视剧：8为数字/空 书名号 数字 去符号 版本 集数
      if(module.equals("电视剧")){
        for (title <- titleArr) {
          if (title.equals(original_name)) {
            return standard_name + "\t" + module + "\t" + id + "\t" + year + "\t" + crowd + "\t" + region
          }
        }
      }

      //////////////////////////////////////////////////////////////////////////////////
      //动画片
      if(module.equals("动画片")){
        for (title <- titleArr) {
          if (title.equals(original_name)) {
            return standard_name + "\t" + module + "\t" + id + "\t" + year + "\t" + crowd + "\t" + region
          }
        }
      }


      ///头部 动画名称 版本 集数 无关字段
      // //数字 名称  版本  集数
      ////////////////////////////////////////////////////////////////////////////////////////////
      //综艺 //8位数字 书名号 去空格 版本 期数 之  排除：爸爸去哪儿_02 爸爸去哪儿_05
      if(module.equals("综艺")){
        for (title <- titleArr) {
          if (title.equals(original_name)) {
            return standard_name + "\t" + module + "\t" + id + "\t" + year + "\t" + crowd + "\t" + region
          }
        }
      }


    })

    return "#"
  }


  /**
    * 判断是否是数字
    *
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
    *
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
    *
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
    *
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
    *
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


  /**
    * 提取视频名称中的集数（酷开）
    */
  def extractVideoPartOFCooCaa(videoName: String): String = {

    var result = "unknow"
    //    println(videoName)

    if (videoName.contains("大结局")) {
      result = "大结局"
    }

    if (videoName.contains("先导集")) {
      result = "1"
    }

    var matched = "\\_[\\s\\S]*".r.findFirstMatchIn(videoName)

    if (!matched.isEmpty)
      result = matched.get.toString().replaceAll("\\_", "").trim


    //////////////////////////////////////////////////////////////////

    matched = "第[\\s\\S]*集".r.findFirstMatchIn(videoName)

    if (!matched.isEmpty)
      result = matched.get.toString().replaceAll("第|集|_", "").trim


    ////////////////////////////////////////////////////////////

    matched = "季[\\s\\S]{0,10}".r.findFirstMatchIn(result)

    if (!matched.isEmpty)
      result = matched.get.toString().replaceAll("季|_", "").trim

    //20160905企鹅爱地球(17)
    //regex4
    val luomaNumMap = Constant.luomaNumMap
    luomaNumMap.keys.foreach(i =>
      result = result.replaceAll(i, luomaNumMap.get(i).get)
    )

    val zhNumMap = Constant.zhNumMap
    zhNumMap.keys.foreach(i =>
      result = result.replaceAll(i, zhNumMap.get(i).get)
    )


    //regex5

    //regex6


    //    if (!matched.isEmpty) {
    //      result = matched.get.toString().replaceAll("第", "").replaceAll("集", "").trim
    //
    //      if (result.contains("季")) {
    //        result = result.substring(result.indexOf("季") + 1).trim
    //      } else {
    //        result = result.substring(1)
    //      }
    //
    //    }
    result

  }


  def convertTitle2Keyword(title: String): String = {
    val keywordArr = Array[String](
      "国语版", "英语版", "粤语版", "日语版", "中文版", "韩语版", "四川话版", "东北话版", "天津话版", "日配版", "云南话版", "兰州话版", "潮汕话版", "陕西话版", "闽南语版", "上海话版", "中配版", "法语版", "多语言版", "话混搭版", "卫视版", "湖南卫视版", "DVD版", "网络版", "央视版", "浙江卫视版", "东方卫视版", "TVB版", "安徽卫视版", "旅游卫视版", "web版", "江西卫视版", "Q版", "OVA版", "FLASH版", "未删减版", "完整版", "全集版", "完全版", "标准版", "真人版", "特别版", "原版", "原声版", "清正版", "重制版", "高清版", "国际版", "独家抢鲜版", "免费版", "短剧版", "搜狐版", "字幕版", "特效重制版", "终极版", "明星版", "配音版", "重映版", "晚间版", "影院版", "新编集版", "分集版", "长篇版", "粉丝定制版", "现场版", "儿歌版", "夜间版", "普通版", "导演版", "抢鲜版", "整合版", "高清正版", "无悔版", "超长版", "现实版", "古代版", "演示版", "国画版", "影像版", "水墨版", "预告版", "翻唱版", "精华短剧版", "阿狸版", "旧版", "合唱版", "口琴版", "舔屏版", "沙画版", "短篇版", "世界版", "合集版", "三次元版", "大陆版", "美国版", "韩国版", "中国版", "英国版", "香港版", "浙江版", "海外版", "内地版", "台湾版", "哥伦比亚版", "伊朗版", "潮汕版", "西班牙版", "意大利版", "希腊版", "四川版", "德国版", "泰国版", "新加坡版", "电影版", "动漫版", "电视剧版", "精编版", "加长版", "纪念版", "经典版", "精简版", "纯享版", "定制版", "混剪版", "贺岁版", "典藏版", "教学版", "独家未播版", "周末版", "周间版", "日播版", "周播版", "清晰版", "蓝光版", "标清版", "3D版", "VR版", "4K版"
    )

    var result = title

    for (keyword <- keywordArr) {
      result = result.replaceAll(keyword, "")
    }

    result
  }

  def isNullorEmptyHandle(str: String): String = {
    var result = str
    if (str == null || str.trim.isEmpty) {
      result = "unknow"
    }
    result
  }

  /**
    * 从书名号中提取书名
    *
    * @param str
    * @return
    */
  def extractByBookMark(str: String): String = {
    ///\《([^》《]*)\》/ig
    val p = Pattern.compile("《(.+?)》")
    val m = p.matcher(str)

    while (m.find()) {
      m.group(1)
    }

    "unknow"
  }


  def main(args: Array[String]): Unit = {

    var res = true
    res = isContainsMessyCode("??????asdfasdf")

    //    res = isContainsMessyCode("呵呵呵呵呵呵")
//    Array([2020,2020,动画片,4, ,少儿, ], [蜜蜜和丽莎的魔幻旅程,蜜蜜和丽莎的魔幻旅程,动画片,21670, ,少儿, ], [『剧集』蜜蜜与莉莎的魔幻旅程,蜜蜜与莉莎的魔幻旅程,动画片,21671,2015,少儿,欧美], [“做张贺卡送祖国”fun秀进校园,“做张贺卡送祖国”fun秀进校园,动画片,23, ,少儿, ], [面包超人 咪嘉与魔法灯,面包超人：咪嘉与魔法灯,动画片,21694, ,少儿, ], [『剧集』面粉镇的节日,面粉镇的节日,动画片,21706,2008,少儿,中国大陆], [1001个玩意儿,1001个玩意儿,动画片,42, ,少儿, ], [面具熊,面具熊,动画片,21709, ,少儿, ], [面具战士,面具战士,动画片,21710, ,少儿,中国大陆], [『剧集』面具战士,面具战士,动画片,21710,2014,少儿,中国大陆], [喵星人V5动作戏,喵星人V5动作戏,动画片,21717, ,少儿, ], [喵星人的那些小破事儿,喵星人的那些小破事儿,动画片,21718, ,少儿, ], [妙趣森林,妙趣森林,动画片,21744, ,少儿, ], [妙音动漫系列,妙音动漫系列,动画片,21755, ,少儿, ], [『剧集』1到2岁绘本故事,1到2岁绘本故事,动画片,114,2013,少儿,中国大陆], [『剧集』1至2岁宝宝好习惯,1至2岁宝宝好习惯,动画片,115,2013,少儿,中国大陆], [『剧集』名画神剪历险记,名画神剪历险记,动画片,21783,2013,少儿,中国大陆], [名人爆料童年趣事,名人爆料童年趣事,动画片,21797, ,少儿, ]


    println(res)
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
