package cn._51doit.spark.day04

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LocationIncome {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据创建RDD
    val lines: RDD[String] = sc.textFile(args(0))

    //对数据进行整理（将数据从字符串转成JSON对象）
    val orderRdd: RDD[OrderBean] = lines.map(line => {
      //转JSON
      var orderBean: OrderBean = null
      try {
        orderBean = JSON.parseObject(line, classOf[OrderBean])
      } catch {
        case e: JSONException => {
          //e.printStackTrace()
          //记录log
          //将问题数据写入到一个存储系统中
        }
      }
      orderBean
    })

    //过滤数据
    val filtered: RDD[OrderBean] = orderRdd.filter(_ != null)

    //关联维度数据
    val beanRDD = filtered.mapPartitions(it => {
      val httpclient = HttpClients.createDefault
      //迭代每个分区中的数据
      val nIter = it.map(bean => {
        val longitude = bean.longitude
        val latitude = bean.latitude
        val httpGet = new HttpGet(s"https://restapi.amap.com/v3/geocode/regeo?&location=$longitude,$latitude&key=4924f7ef5c86a278f5500851541cdcff")
        val response = httpclient.execute(httpGet)
        try {
          //System.out.println(response.getStatusLine)
          val entity1 = response.getEntity
          // do something useful with the response body
          // and ensure it is fully consumed
          var province: String = null
          var city: String = null
          if (response.getStatusLine.getStatusCode == 200) {
            //获取请求的json字符串
            val result = EntityUtils.toString(entity1)

            //转成json对象
            val jsonObj = JSON.parseObject(result)
            //获取位置信息
            val regeocode = jsonObj.getJSONObject("regeocode")
            if (regeocode != null && !regeocode.isEmpty) {
              val address = regeocode.getJSONObject("addressComponent")
              //获取省市区
              bean.province = address.getString("province")
              bean.city = address.getString("city")
            }
          }
        } finally {
          response.close()
        }
        if(!it.hasNext) {
          //关闭连接
          httpclient.close()
        }
        bean
      })
      nIter
    })

    val result = beanRDD.map(bean => (bean.province, bean.money)).reduceByKey(_ + _)
    val r = result.collect()
    println(r.toBuffer)
    sc.stop()

  }
}
