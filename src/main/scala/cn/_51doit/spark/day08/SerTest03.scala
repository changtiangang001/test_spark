package cn._51doit.spark.day08

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object SerTest03 {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(1))

    //传入Map的函数是在Driver定义的，但是是在Executor调用执行的
    val tpRdd: RDD[(String, String, Int, Long, String, String)] = lines.map(w => {
      val rulesMap = new RulesMapClassNotSer //每处理一条数据就会new一个实例
      //闭包（rulesMap要在Executor被使用）
      val province = rulesMap.rules.getOrElse(w, "未知")
      val taskId = TaskContext.getPartitionId()
      val threadId = Thread.currentThread().getId
      val hostName = InetAddress.getLocalHost.getHostName
      (w, province, taskId, threadId, hostName, rulesMap.toString)
    })

    tpRdd.saveAsTextFile(args(2))

    sc.stop()

  }
}
