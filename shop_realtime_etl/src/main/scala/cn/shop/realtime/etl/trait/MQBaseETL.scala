package cn.shop.realtime.etl.`trait`
import cn.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @Package cn.shop.realtime.etl.`trait`
 * @File ：MQBaseETL.java
 * @author 大数据老哥
 * @date 2021/3/20 15:44
 * @version V1.0
 */
 abstract  class MQBaseETL(env:StreamExecutionEnvironment) extends BaseETL [String]{
  /**
   * 根据业务可以抽出来kafka读取方法，因为所有的ETL都会操作kafka
   *
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String): DataStream[String] = {
    // 创建kafka对象，从kafka中消费数据，消费到的数据都是字符串类型
    val kafkaProducer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), KafkaProps.getKafkaProperties())
    // 将消费这对象添加到数据源中
    env.addSource(kafkaProducer)
  }

}
