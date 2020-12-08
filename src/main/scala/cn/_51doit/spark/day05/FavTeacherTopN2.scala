package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherTopN2 {

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

    val subjects = Array("bigdata", "javaee", "php")

    for(sub <- subjects) {
      //对数据进行过滤
      val bigdataRDD: RDD[((String, String), Int)] = reduced.filter(_._1._1.equals(sub))
      implicit val orderRules: Ordering[((String, String), Int)] = Ordering[Int].on[((String, String), Int)](t => t._2)
      val res = bigdataRDD.top(topN) //Action
      println(res.toBuffer)
    }

    sc.stop()

  }
}
