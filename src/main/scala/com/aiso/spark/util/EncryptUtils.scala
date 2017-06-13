package com.aiso.spark.util

import java.security.MessageDigest

object EncryptUtils {

  def main(args: Array[String]) {
    val text = "hahaha"
    println(toMd5One(text))
    println(toMd5Two(text))
    println(md5Hash(text))

  }

  def toMd5One(s: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }


  def toMd5Two(text: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(text.getBytes).map("%02x".format(_)).mkString

  }

//  def toMd5Three(text: String): String = {
//    val digest = MessageDigest.getInstance("MD5")
//    //    digest.update("MD5 ".getBytes())
//    //    digest.update("this ".getBytes())
//    //    digest.update("text!".getBytes())
//    digest.digest().map(0xFF & _).map("%02x".format(_)).mkString
//  }

  def md5Hash(text: String): String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
}
