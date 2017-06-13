package com.aiso.spark.util

import java.util.regex.Pattern

object RegexUtils {

  def getPlayModelByLogTitle(title: String): String = {


    ////电视剧：8为数字/空 书名号 数字 去符号 版本 集数
    val dsjRegex = "^(\\d{8})(.+)(([ ]\\d|_\\d|\\(\\d\\)|-第\\d(集|季)|第\\d(集|季)|\\(第\\d(集|季)\\)|大结局|先导集){1})([^集季]*)$".r
    if(!dsjRegex.findFirstMatchIn(title).isEmpty){
         return "电视剧"
    }

    //数字 名称  版本  集数
    val dhRegex = "^(\\d+)(.+)(([ ]\\d|_\\d|\\(\\d\\)|-第\\d(集)|第\\d(集)|\\(第\\d(集)\\)|大结局|先导集){1})([^集]*)$".r
    if(!dhRegex.findFirstMatchIn(title).isEmpty){
      return "动画片"
    }

    //8位数字 书名号 去空格 版本 期数 之  排除：爸爸去哪儿_02 爸爸去哪儿_05
    val zyRegex = "^(\\d{8})(.+)(([ ]\\d|_\\d|\\(\\d\\)|-第\\d(集|季|期)|第\\d(集|季|期)|\\(第\\d(集|季|期)\\)|大结局|先导集){1}).*$".r
    if(!zyRegex.findFirstMatchIn(title).isEmpty){
      return "综艺"
    }

    return "未知"

  }

  def main(args: Array[String]) {
      var res = getPlayModelByLogTitle("20160724幻城未删减版第51集")
      println(res)
  }

  def test01(str: String): String = {
    //查找以Java开头,任意结尾的字符串
    val pattern = Pattern.compile("^Java.*");
    val matcher = pattern.matcher("Java不是人");
    val b = matcher.matches();
    //当条件满足时，将返回true，否则返回false
    System.out.println(b);
    ""
  }

  def test02(str: String): String = {
    //查找以Java开头,任意结尾的字符串
    val pattern = Pattern.compile("^Java.*");
    val matcher = pattern.matcher("Java不是人");
    val b = matcher.matches();
    //当条件满足时，将返回true，否则返回false
    System.out.println(b);
    ""
  }

  def test03(str: String): String = {
    //查找以Java开头,任意结尾的字符串
    val pattern = Pattern.compile("^Java.*");
    val matcher = pattern.matcher("Java不是人");
    val b = matcher.matches();
    //当条件满足时，将返回true，否则返回false
    System.out.println(b);
    ""
  }

  def test04(str: String): String = {
    //查找以Java开头,任意结尾的字符串
    val pattern = Pattern.compile("^Java.*");
    val matcher = pattern.matcher("Java不是人");
    val b = matcher.matches();
    //当条件满足时，将返回true，否则返回false
    System.out.println(b);
    ""
  }

}
