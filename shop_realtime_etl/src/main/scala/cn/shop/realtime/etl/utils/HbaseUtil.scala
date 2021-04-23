package cn.shop.realtime.etl.utils

import cn.shop.realtime.etl.utils.pool.{ConnectionPoolConfig, HbaseConnectionPool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * hbase连接池工具类
 */
object HbaseUtil {
  val config = new ConnectionPoolConfig
  //最大连接数, 默认20个
  config.setMaxTotal(20)
  //最大空闲连接数, 默认20个
  config.setMaxIdle(5)
  //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认1000
  config.setMaxWaitMillis(1000)
  //在获取连接的时候检查有效性, 默认false
  config.setTestOnBorrow(true)

  var hbaseConfig: Configuration = HBaseConfiguration.create
  hbaseConfig = HBaseConfiguration.create
  hbaseConfig.set("hbase.defaults.for.version.skip", "true")
  //创建连接池对象
  lazy val pool: HbaseConnectionPool = new HbaseConnectionPool(config, hbaseConfig)

  //获取连接池对象
  def getPool() ={
    pool
  }

}
