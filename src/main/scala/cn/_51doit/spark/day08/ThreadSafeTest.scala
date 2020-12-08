package cn._51doit.spark.day08

import java.net.InetAddress
import java.text.SimpleDateFormat

import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object ThreadSafeTest {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }


    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(1))

//    lines.map(line => {
//      val timestamp: Long = DateUtils.parse(line)
//      (line, timestamp)
//    }).foreach(println)

    lines.mapPartitions(it => {
      val dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(line => {
        val timestamp = dfs.parse(line).getTime
        (timestamp, line)
      })
    }).foreach(println)


    sc.stop()

  }
}
