package cn._51doit.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setAppName("WordCount")
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD(Lazy)
    val lines: RDD[String] = sc.textFile(args(0))


    //Transformation 开始(Lazy)
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //Transformation 结束

    //Action算子，会触发任务执行
    //将数据保存到HDFS
    reduced.saveAsTextFile(args(1))

    //释放资源
    sc.stop()
  }
}
