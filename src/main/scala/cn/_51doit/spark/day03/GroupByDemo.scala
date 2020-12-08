package cn._51doit.spark.day03

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GroupByDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    val words: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)

    //words.groupBy()

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //调用groupBy进行分组
    //val grouped: RDD[(String, Iterable[(String, Int)])] = wordAndOne.groupBy(_._1)
    val grouped = wordAndOne.map(x => (x._1, x)).groupByKey()

    grouped.saveAsTextFile("groupby-out2")

    sc.stop()

  }
}
