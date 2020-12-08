package cn._51doit.spark.day08

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

//  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//  不建议使用，因为加锁，速度会变慢
//  def parse(dateStr: String): Long = synchronized {
//    val date = sdf.parse(dateStr)
//    date.getTime
//  }


    val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    def parse(dateStr: String): Long = synchronized {
      val date = sdf.parse(dateStr)
      date.getTime
    }


}
