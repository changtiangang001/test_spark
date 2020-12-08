package cn._51doit.spark.day09

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

object IpRulesLoader {

  //使用IO流读数据，然后放入到一个ArrayBuffer

  //在object中定义的定义的数据是静态的，在一个JVM进程中，只有一份
  val ipRules = new ArrayBuffer[(Long, Long, String, String)]()
  //加载IP规则数据，在Executor的类加载是执行一次
  //静态代码块
  //读取HDFS中的数据
  //val fileSystem = FileSystem.get(URI.create("file://"), new Configuration())
  //val inputStream = fileSystem.open(new Path("/Users/xing/Desktop/ip.txt"))
  val bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("data/ip.txt")))
  var line: String = null
  do {
    line = bufferedReader.readLine()
    if (line != null) {
      //处理IP规则数据
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      val t = (startNum, endNum, province, city)
      ipRules += t
    }
  } while (line != null)


  def getAllRules: ArrayBuffer[(Long, Long, String, String)] = {
    ipRules
  }

}
