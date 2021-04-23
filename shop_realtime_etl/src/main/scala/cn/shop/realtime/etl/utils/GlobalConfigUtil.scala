package cn.shop.realtime.etl.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @Package cn.shop.realtime.etl.utils
 * @File ：GlobalConfigUtil.java
 * @author 大数据老哥
 * @date 2021/3/19 18:10
 * @version V1.0
 */
object GlobalConfigUtil {

  private val config: Config = ConfigFactory.load()

  val `bootstrap.servers` = config.getString("bootstrap.servers")
  val `zookeeper.connect` = config.getString("zookeeper.connect")
  val `input.topic.canal` = config.getString("input.topic.canal")
  val `input.topic.click_log` = config.getString("input.topic.click_log")
  val `input.topic.comments` = config.getString("input.topic.comments")
  val `group.id` = config.getString("group.id")
  val `enable.auto.commit` = config.getString("enable.auto.commit")
  val `auto.commit.interval.ms` = config.getString("auto.commit.interval.ms")
  val `auto.offset.reset` = config.getString("auto.offset.reset")
  val `key.serializer` = config.getString("key.serializer")
  val `key.deserializer` = config.getString("key.deserializer")
  val `output.topic.order` = config.getString("output.topic.order")
  val `output.topic.order_detail` = config.getString("output.topic.order_detail")
  val `output.topic.cart` = config.getString("output.topic.cart")
  val `output.topic.clicklog` = config.getString("output.topic.clicklog")
  val `output.topic.goods` = config.getString("output.topic.goods")
  val `output.topic.ordertimeout` = config.getString("output.topic.ordertimeout")
  val `output.topic.comments` =  config.getString("output.topic.comments")
  val `hbase.table.orderdetail` = config.getString("hbase.table.orderdetail")
  val `hbase.table.family` = config.getString("hbase.table.family")
  val `redis.server.ip` = config.getString("redis.server.ip")
  val `redis.server.port`: String = config.getString("redis.server.port")
  val `ip.file.path` = config.getString("ip.file.path")
  val `mysql.server.ip` = config.getString("mysql.server.ip")
  val `mysql.server.port` = config.getString("mysql.server.port")
  val `mysql.server.database` = config.getString("mysql.server.database")
  val `mysql.server.username` = config.getString("mysql.server.username")
  val `mysql.server.password` = config.getString("mysql.server.password")
  val `input.topic.cart` = config.getString("input.topic.cart")

  def main(args: Array[String]): Unit = {
    println(`input.topic.cart`)
  }
}
