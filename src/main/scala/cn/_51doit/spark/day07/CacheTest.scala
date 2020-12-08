package cn._51doit.spark.day07

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object CacheTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://node-1.51doit.com:9000/chk0813")

    val lines: RDD[String] = sc.textFile(args(0))

    val filtered: RDD[String] = lines.filter(_.endsWith("laozhao"))

    filtered.cache()

    filtered.checkpoint()

    filtered.count()

    sc.stop()

  }
}
