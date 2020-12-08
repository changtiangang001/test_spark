package cn._51doit.spark.day05

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherTopN1 {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据创建RDD
    val lines = sc.textFile(args(1))

    val topN = args(2).toInt

    //对数据进行整理
    val reduced: RDD[((String, String), Int)] = lines.map(line => {
      val fields = line.split("/")
      val url = fields(2)
      val teacher = fields(3)
      val subject = url.split("[.]")(0)
      ((subject, teacher), 1)
    }).reduceByKey(_ + _)

    //对数据进行分组
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    val result: RDD[(String, String, Int)] = grouped.flatMapValues(it => {
      //如果迭代器中有大量数据，toList会内存溢出
      it.toList.sortBy(-_._2).take(topN)
    }).map(t => {
      (t._1, t._2._1._2, t._2._2)
    })

    println(result.collect().toBuffer)

    sc.stop()

  }
}
