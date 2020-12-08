package cn._51doit.spark.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort4 {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(List("laoduan,30,99.99", "nianhang,28,99.99", "laozhao,18,9999.99"))

    val tfboy: RDD[(String, Int, Double)] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      (name, age, fv)
    })

    val sorted: RDD[(String, Int, Double)] = tfboy.sortBy(t => (-t._3, t._2))

    println(sorted.collect().toBuffer)

    sc.stop()

  }

}
