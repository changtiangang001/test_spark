package cn._51doit.spark.day05

import scala.io.Source

object IteratorDemo {

  def main(args: Array[String]): Unit = {

    val lines: Iterator[String] = Source.fromFile("data/words.txt").getLines()

    val filtered: Iterator[String] = lines.filter(line => {
      println("filter")
      line.startsWith("h")
    })

    val up: Iterator[String] = filtered.map(line => {
      println("map")
      line.toUpperCase()
    })

    //println(up)

//    up.foreach(w => {
//      println(w)
//    })

    val array = up.toArray

    println(array)
  }

}
