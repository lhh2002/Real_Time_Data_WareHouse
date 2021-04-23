package cn.shop.realtime.etl.process

import cn.shop.realtime.etl.`trait`.MQBaseETL
import cn.shop.realtime.etl.bean.{ClickLogBean, ClickLogWideEntity}
import cn.shop.realtime.etl.utils.DateUtil.{date2DateStr, datetime2date}
import cn.shop.realtime.etl.utils.{DateUtil, GlobalConfigUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.canal.util.ip.IPSeeker
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import java.io.File

/**
 * @Package cn.shop.realtime.etl.app
 * @File ：ClickLogDataETL.java
 * @author 大数据老哥
 * @date 2021/4/3 15:48
 * @version V1.0
 */
class ClickLogDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {


  /**
   * 根据业务抽取出process方法，因为所有的ETL都有操作方法
   *
   */
  override def process(): Unit = {
    /**
     * 实现步骤：
     * 1. 获取点击流的数据源
     * 2. 将nginx的点击流日志字符串转为成点击流对象
     * 3.对点击流的日志进行实时拉宽操作，返回拉宽后的点击流实体对象
     * 4.将拉宽后的点击流实体转换成json字符串
     * 5. 将json字符串写入到kafka集群，提供Druid进行实时的拉去操作
     */
    // 获取点击流的数据源
    val clickLogDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.click_log`)
    //
    val clickLogWideDataStream = etl(clickLogDataStream)
    // 将对象中的数据转换为json字符串写入到kafka中
    val clickLogDataStreamJson: DataStream[String] = clickLogWideDataStream.map(log => {

      JSON.toJSONString(log,SerializerFeature.DisableCircularReferenceDetect)

    })
     clickLogDataStreamJson.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.clicklog`))
  }

  def etl(clickLogDataStream: DataStream[String]) = {

    val clickLogBeanDataStream: DataStream[ClickLogBean] = clickLogDataStream.map(new RichMapFunction[String, ClickLogBean] {
      // 定义解析器
      var parser: HttpdLoglineParser[ClickLogBean] = _

      override def open(parameters: Configuration): Unit = {
        parser = ClickLogBean.createClickLogParser()

      }

      override def map(value: String): ClickLogBean = {
        ClickLogBean(parser, value)
      }
    })
    val clickLogWideDataStream = clickLogBeanDataStream.map(new RichMapFunction[ClickLogBean, ClickLogWideEntity] {

      // 定义ip 获取省份和城市
      var ipSeeker: IPSeeker = _

      override def open(parameters: Configuration): Unit = {
        // 获取分布式缓存文件
        val dataFile: File = getRuntimeContext.getDistributedCache.getFile("qqwry.day")
        ipSeeker = new IPSeeker(dataFile)
      }

      override def map(value: ClickLogBean): ClickLogWideEntity = {
        val clickLogWideEntity = ClickLogWideEntity(value)
        // 根据ip地址获取省份，城市信息
        val country = ipSeeker.getCountry(clickLogWideEntity.getIp)
        var areaArray = country.split("省")
        if (areaArray.length > 1) {
          // 表示非直辖市
          clickLogWideEntity.province = areaArray(0) + "省"
          clickLogWideEntity.city = areaArray(1)
        } else {
          // 表示直辖市
          areaArray = country.split("市")
          if (areaArray.length > 1) {
            clickLogWideEntity.province = areaArray(0) + "市"
            clickLogWideEntity.city = areaArray(1)
          } else {
            clickLogWideEntity.province = areaArray(0) + "市"
            clickLogWideEntity.city = ""
          }
        }
        clickLogWideEntity.requestDataTime = date2DateStr(datetime2date(clickLogWideEntity.getRequestTime), "yyyy-MM-dd HH:mm:ss")

        clickLogWideEntity
      }
    })
    clickLogWideDataStream

  }
}
