package cn._51doit.spark.day03

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object FoldByKeyDemo {

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

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //wordAndOne.reduceByKey(_+_)
    val result = wordAndOne.foldByKey(100)(_ + _)

    result.saveAsTextFile("fold2-out")
    sc.stop()

  }
}
