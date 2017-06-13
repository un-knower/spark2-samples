package com.aiso.spark.util

import java.io.{File, FileInputStream, FileOutputStream, IOException}

import com.aiso.spark.common.Helper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.io.IOUtils


/**
  * hdfs文件夹操作类
  */
object HdfsUtils {

  def getFS(): FileSystem = {
    //    System.setProperty("hadoop.home.dir", "D:\\04coding\\projects-bigData\\Hadoop\\hadoop-2.5.0")
    System.setProperty("HADOOP_USER_NAME", "hdfs")


    //hadoop 的方式
    //    val conf = new Configuration()
    //    conf.set("fs.defaultFS", "hdfs://srv5.avcdata.com:8020/")
    //    //    conf.set("fs.defaultFS", "hdfs://srv8.avcdata.com:8020/")
    //    conf.set("mapred.remote.os", "Linux")
    //    FileSystem.get(conf)

    //spark 方式
    val hadoopConf = Helper.sparkContext.hadoopConfiguration
     FileSystem.get(hadoopConf)
  }

  /**
    * 关闭FileSystem
    *
    * @param fileSystem
    */
  def closeFS(fileSystem: FileSystem) {
    if (fileSystem != null) {
      try {
        fileSystem.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }


  ///////////////////////////////////////////////////////////////////////////


  /**
    * ls
    *
    * @param hdfsFilePath
    */
  def listFiles(hdfsFilePath: String): Unit = {
    val fileSystem = getFS()
    val fstats = fileSystem.listStatus(new Path(hdfsFilePath))
    try {

      for (fstat <- fstats) {
        if (fstat.isDirectory()) {
          println("directory")
        } else {
          println("file")
        }
        println("Permission:" + fstat.getPermission())
        println("Owner:" + fstat.getOwner())
        println("Group:" + fstat.getGroup())
        println("Size:" + fstat.getLen())
        println("Replication:" + fstat.getReplication())
        println("Block Size:" + fstat.getBlockSize())
        println("Name:" + fstat.getPath())
        println("#############################")
      }

    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (fileSystem != null) {
        try {
          fileSystem.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }

  def ls(fileSystem: FileSystem, path: String) = {
    println("list path:" + path)
    val fs = fileSystem.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    for (p <- listPath) {
      println(p)
    }
    println("----------------------------------------")
  }


  /**
    * 创建目录
    *
    * @param hdfsFilePath
    */
  def mkdir(hdfsFilePath: String) = {
    val fileSystem = getFS()

    try {
      val success = fileSystem.mkdirs(new Path(hdfsFilePath))
      if (success) {
        println("Create directory or file successfully")
      }
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }

  /**
    * 删除文件或目录
    *
    * @param hdfsFilePath
    * @param recursive 递归
    */
  def rm(hdfsFilePath: String, recursive: Boolean): Unit = {
    val fileSystem = this.getFS()
    val path = new Path(hdfsFilePath)
    try {
      if (fileSystem.exists(path)) {
        val success = fileSystem.delete(path, recursive)
        if (success) {
          System.out.println("delete successfully")
        }
      }

    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }


  /**
    * 上传文件到HDFS
    *
    * @param localPath
    * @param hdfspath
    */
  def write(localPath: String, hdfspath: String) {

    val inStream = new FileInputStream(
      new File(localPath)
    )
    val fileSystem = this.getFS()
    val writePath = new Path(hdfspath)
    val outStream = fileSystem.create(writePath)

    try {
      IOUtils.copyBytes(inStream, outStream, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
      IOUtils.closeStream(outStream)
    }

  }

  /**
    * //    * 上传文件到HDFS
    * //    *
    * //    * @param localFilePath
    * //    * @param hdfsFilePath
    * //    */
  //  def put(localFilePath: String, hdfsFilePath: String) = {
  //    val fileSystem = this.getFS()
  //    try {
  //      val fdos = fileSystem.create(new Path(hdfsFilePath))
  //      val fis = new FileInputStream(new File(localFilePath))
  //      IOUtils.copyBytes(fis, fdos, 1024)
  //
  //    } catch {
  //      case e: IllegalArgumentException => e.printStackTrace()
  //      case e: IOException => e.printStackTrace()
  //    } finally {
  //      IOUtils.closeStream(fileSystem)
  //    }
  //  }


  /**
    * 打印hdfs上的文件内容
    *
    * @param hdfsFilePath
    */
  def cat(hdfsFilePath: String) {

    val fileSystem = this.getFS()

    val readPath = new Path(hdfsFilePath)

    val inStream = fileSystem.open(readPath)

    try {
      IOUtils.copyBytes(inStream, System.out, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
    }
  }


  /**
    * 下载文件到本地
    *
    * @param localFilePath
    * @param hdfsFilePath
    */
  def get(localFilePath: String, hdfsFilePath: String) {
    val fileSystem = this.getFS()
    try {
      val fsis = fileSystem.open(new Path(hdfsFilePath))
      val fos = new FileOutputStream(new File(localFilePath))
      IOUtils.copyBytes(fsis, fos, 1024)
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(fileSystem)
    }
  }


  def main(args: Array[String]) {

    val fileSystem = getFS()
    val path = "/user/hdfs/rsync/CH/2016-12-14"

    try {

      //      println("list path:---------" + path)
      //      val fs = fileSystem.listStatus(new Path(path))
      //      val listPath = FileUtil.stat2Paths(fs)
      //      for (p <- listPath) {
      //        println(p)
      //      }
      //      println("----------------------------------------")

      //      val fdis = fileSystem.open(new Path("/user/hdfs"))
      //      IOUtils.copyBytes(fdis, System.out, 1024)


      val fstats = fileSystem.listStatus(new Path(path))
      for (fstat: FileStatus <- fstats) {
        //        println(fstat.isDirectory() ? "directory": "file")
        //        println("Permission:" + fstat.getPermission())
        //        println("Owner:" + fstat.getOwner())
        //        println("Group:" + fstat.getGroup())
        val path = fstat.getPath().toString
        val name = path.substring(path.toString.lastIndexOf("/") + 1)
        println(name)
        //        println("Size:" + fstat.getLen/1024/1024)
        //        println("Replication:" + fstat.getReplication())
        //        println("Block Size:" + fstat.getBlockSize())

        //        println("#############################")
      }

    } catch {
      case ex: IOException => {
        ex.printStackTrace()
        println(ex.getCause)
        println("link err")
      }
    } finally {
      IOUtils.closeStream(fileSystem)
    }


  }
}