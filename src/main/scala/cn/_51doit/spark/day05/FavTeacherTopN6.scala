package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object FavTeacherTopN6 {

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
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val fields = line.split("/")
      val url = fields(2)
      val teacher = fields(3)
      val subject = url.split("[.]")(0)
      ((subject, teacher), 1)
    })

    val subjects = subjectTeacherAndOne.map(_._1._1).distinct().collect()

    //初始化分区
    val subjectPartitioner = new SubjectPartitioner(subjects)

    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(subjectPartitioner, _ + _)

    val result: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //定义一个可以排序的特殊集合
      implicit val rules = Ordering[Int].on[((String, String), Int)](t => -t._2)
      val sorter = new mutable.TreeSet[((String, String), Int)]()
      //遍历迭代器
      it.foreach(t => {
        //将数据添加到treesetzhong
        sorter += t
        if (sorter.size > topN) {
          sorter -= sorter.last
        }
      })
      sorter.iterator
    })

    println(result.collect().toBuffer)

    Thread.sleep(1000000)




  }
}

