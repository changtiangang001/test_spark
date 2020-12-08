package cn._51doit.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocalDebug {

  def main(args: Array[String]): Unit = {


    //System.setProperty("HADOOP_USER_NAME", "root")
    //创建SparkContext
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]") //本地模式，用来测试的，不会跟集群建立连接，就一个本地的进程运行
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD(Lazy)
    val lines: RDD[String] = sc.textFile(args(0))

    //Transformation 开始(Lazy)
    //切分压平
    val words: RDD[String] = lines.flatMap(line => {
      val arr = line.split(" ")
      arr
    })

    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //Transformation 结束

    //Action算子，会触发任务执行
    //将数据保存到HDFS
    sorted.saveAsTextFile(args(1))

    //释放资源
    sc.stop()
  }
}
