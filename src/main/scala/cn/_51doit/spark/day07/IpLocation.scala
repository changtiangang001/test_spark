package cn._51doit.spark.day07

import cn._51doit.spark.utils.IpUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    //先读取iP规则数据
    val ipRuleLines: RDD[String] = sc.textFile(args(1))

    val ipRulesInDriver: Array[(Long, Long, String, String)] = ipRuleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      (startNum, endNum, province, city)
    }).collect //将ip规则收集到driver

    //将dirver端准备好的数据广播的Executor中
    //返回一个广播变量的引用（是在driver端），该方法是一个阻塞的方法
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRulesInDriver)

    //读取访问日志数据
    val accessLog: RDD[String] = sc.textFile(args(2))

    //对访问日志进行整理
     val provinceAndOne: RDD[(String, Int)] = accessLog.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      val ipNum = IpUtils.ip2Long(ip)
      //在Executor中关联事先广播好的数据
      //通过广播变量的引用，可以获取到事先广播到Exector中的数据
      val ipRulesInExecutor: Array[(Long, Long, String, String)] = broadcastRef.value
      //使用二分法查找
      val index = IpUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1) {
        province = ipRulesInExecutor(index)._3
      }
      (province, 1)
    })

    val res = provinceAndOne.reduceByKey(_ + _)

    println(res.collect().toBuffer)

    sc.stop()


  }
}
