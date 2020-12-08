package cn._51doit.spark.day09

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import cn._51doit.spark.utils.IpUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object IpLocationV3 {

  private val logger: Logger = LoggerFactory.getLogger(IpLocationV3.getClass)

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    val accessLog: RDD[String] = sc.textFile(args(1))

    val reduced = accessLog.mapPartitions(it => {
      //初始化IPLoader
      val allRulesInExecutor: ArrayBuffer[(Long, Long, String, String)] = IpRulesLoader.getAllRules
      it.map(line => {
        val fields = line.split("[|]")
        val ip = fields(1)
        val ipNum = IpUtils.ip2Long(ip)
        val index = IpUtils.binarySearch(allRulesInExecutor, ipNum)
        var province = "未知"
        if (index != -1) {
          province = allRulesInExecutor(index)._3
        }
        (province, 1)
      })
    }).reduceByKey(_ + _)

    //将数据收集到Driver端在写入到MySQL、Redis、Hbase
    //collect到Driver端的数据不能太大，数据收到是通过网络，数据量大会效率低，会丢失数据
    //val res = reduced.collect()


    //假设这个数据量大，在Executor中写入数据
    //    reduced.foreach(t => {
    //      //创建一个数据库连接
    //      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123456")
    //      val pstm = connection.prepareStatement("INSERT INTO daily_ip_count (dt, province, counts) VALUES (?, ?, ?)")
    //      pstm.setDate(1, new Date(System.currentTimeMillis()))
    //      pstm.setString(2, t._1)
    //      pstm.setInt(3, t._2)
    //      pstm.executeUpdate()
    //
    //      pstm.close()
    //      connection.close()
    //    })

    reduced.foreachPartition(it => {
      var connection: Connection = null
      var pstm: PreparedStatement = null
      try {
        var i = 0
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123456")
        pstm = connection.prepareStatement("INSERT INTO daily_ip_count (dt, province, counts) VALUES (?, ?, ?)")
        it.foreach(t => {
          pstm.setDate(1, new Date(System.currentTimeMillis()))
          pstm.setString(2, t._1)
          pstm.setInt(3, t._2)
          pstm.addBatch()
          i += 1
          if (i % 100 == 0) {
            pstm.executeLargeBatch()
          }
        })
        pstm.executeLargeBatch()
      } catch {
        case e: Exception => {
          //记录异常日志
          logger.error("写入数据库异常", e)
        }
      } finally {
        if(pstm != null) {
          pstm.close()
        }
        if(connection != null) {
          connection.close()
        }
      }
    })


    sc.stop()


  }

}
