package cn._51doit.spark.day09

import cn._51doit.spark.utils.IpUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocationV2 {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    val accessLog: RDD[String] = sc.textFile(args(1))

    val reduced = accessLog.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      val ipNum = IpUtils.ip2Long(ip)

      //获取Executor中的规则数据
      val allRulesInExecutor = IpRulesLoader.getAllRules

      val index = IpUtils.binarySearch(allRulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1) {
        province = allRulesInExecutor(index)._3
      }
      (province, 1)
    }).reduceByKey(_+_)

    val res = reduced.collect()

    println(res.toBuffer)

    sc.stop()


  }

}
