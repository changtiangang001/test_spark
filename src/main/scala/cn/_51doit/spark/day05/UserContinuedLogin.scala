package cn._51doit.spark.day05

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserContinuedLogin {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据创建RDD
    val lines = sc.textFile(args(1))

    //对数据进行整理
    val uidAndDate: RDD[(String, String)] = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val date = fields(1)
      (uid, date)
    })

    //根据uid进行分组，将同一个用户的登录数据搞到同一个组内
    val grouped: RDD[(String, Iterable[String])] = uidAndDate.groupByKey()

    //在组内进行排序
    val uidAndDateDif: RDD[(String, (String, String))] = grouped.flatMapValues(it => {
      //将迭代器中的数据toList / toSet
      val sorted = it.toSet.toList.sorted
      //定义一个日期的工具类
      val calendar = Calendar.getInstance()
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var index = 0
      sorted.map(dateStr => {
        val date: Date = sdf.parse(dateStr)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -index)
        index += 1
        (dateStr, sdf.format(calendar.getTime))
      })
    })

    val result: RDD[(String, Int, String, String)] = uidAndDateDif.map(t => {
      ((t._1, t._2._2), t._2._1)
    }).groupByKey().mapValues(it => {
      val list = it.toList.sorted
      val times = list.size
      val beginTime = list.head
      val endTime = list.last
      (times, beginTime, endTime)
    }).filter(t => t._2._1 >= 3)
      .map(t => {
        (t._1._1, t._2._1, t._2._2, t._2._3)
      })

    println(result.collect().toBuffer)

    sc.stop()

  }
}
