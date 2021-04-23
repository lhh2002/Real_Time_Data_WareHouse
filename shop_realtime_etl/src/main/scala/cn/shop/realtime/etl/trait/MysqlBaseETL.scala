package cn.shop.realtime.etl.`trait`

import cn.canal.protobuf.CanalModel.RowData
import cn.shop.realtime.etl.utils.{CanalRowDataDeserialzerSchema, GlobalConfigUtil, KafkaProps}
import com.canal.bean.CanalRowData
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @Package cn.shop.realtime.etl.`trait`
 * @File ：MysqlBaseETL.java
 * @author 大数据老哥
 * @date 2021/3/20 19:59
 * @version V1.0
 */
abstract class MysqlBaseETL(env: StreamExecutionEnvironment) extends BaseETL[CanalRowData] {
  /**
   * 根据业务可以抽出来kafka读取方法，因为所有的ETL都会操作kafka
   *
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[CanalRowData] = {

    // 消费kafka中的数据
    val CanalRowData = new FlinkKafkaConsumer011[CanalRowData](
      topic,
      // 使用自定义序列化方式
      new CanalRowDataDeserialzerSchema(),
      KafkaProps.getKafkaProperties()
    )
    //将kafka中的数据返回出去
    env.addSource(CanalRowData)
  }


}
