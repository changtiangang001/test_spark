package cn._51doit.spark.day02

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object GroupByKeyDemo {

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

    val grouped: RDD[(String, Iterable[Int])] = wordAndOne.groupByKey()
    //grouped.saveAsTextFile("out")

    val shuffledRdd: ShuffledRDD[String, Int, ListBuffer[Int]] = new ShuffledRDD[String, Int, ListBuffer[Int]](wordAndOne, new HashPartitioner(wordAndOne.partitions.length))

    val createCombiner = (v: Int) => ListBuffer(v)
    val mergeValue = (buf: ListBuffer[Int], v: Int) => buf += v
    val mergeCombiners = (c1: ListBuffer[Int], c2: ListBuffer[Int]) => c1 ++= c2
    shuffledRdd.setAggregator(new Aggregator[String, Int, ListBuffer[Int]](
      createCombiner,
      mergeValue,
      mergeCombiners
    ))
    shuffledRdd.setMapSideCombine(false)

    shuffledRdd.saveAsTextFile("out")

    sc.stop()

  }
}
