package cn.shop.realtime.etl.`trait`

import cn.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/**
 * @Package cn.shop.realtime.etl.`trait`
 * @File ：BaseETL.java
 * @author 大数据老哥
 * @date 2021/3/20 15:41
 * @version V1.0
 */
trait BaseETL[T] {

  /**
   * 根据业务可以抽出来kafka读取方法，因为所有的ETL都会操作kafka
   * @param topic
   * @return
   */

  def getKafkaDataStream(topic: String): DataStream[T]

  /**
   * 根据业务抽取出process方法，因为所有的ETL都有操作方法
   *
   */
  def process( )

  /**
   * 构建kafka的生产者对象
   * @param topic
   * @return
   */
  def kafkaProducer(topic:String) ={
    new FlinkKafkaProducer011[String](
      topic,
      // 这种方式常用语读取kafka中的数据，在写入将数据写入到kafka的场景
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      KafkaProps.getKafkaProperties()
    )
  }
}
