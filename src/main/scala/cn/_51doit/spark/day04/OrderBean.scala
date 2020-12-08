package cn._51doit.spark.day04

case class OrderBean(
                      oid: String,
                      cid: Int,
                      money: Double,
                      longitude: Double,
                      latitude: Double,
                      var province: String,
                      var city: String
                    )
