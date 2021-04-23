package cn.shop.realtime.etl.process

import cn.canal.protobuf.CanalModel.RowData
import cn.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.shop.realtime.etl.bean.OrderDBEntity
import cn.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.canal.bean.CanalRowData
import org.apache.flink.streaming.api.scala._

/**
 * @Package cn.shop.realtime.etl.app
 * @File ：OrderETL.java
 * @author 大数据老哥
 * @date 2021/4/3 20:18
 * @version V1.0
 *          订单数据的实时ETL
 */
case class OrderETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 根据业务抽取出process方法，因为所有的ETL都有操作方法
   *
   */
  override def process(): Unit = {
    //读取kafka中的数据过滤出order
    val orderDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_orders")
    //将读取来的rowdata转为对象
    val orderDBEntityDataStream = orderDataStream.map(data => {
      OrderDBEntity(data)
    })
    // 将对象转为json
    val orderDataStreamJson = orderDBEntityDataStream.map(row => {
      JSON.toJSONString(row, SerializerFeature.DisableCircularReferenceDetect)
    })
    // 将json写入到kafka中

    orderDataStreamJson.printToErr("订单数据信息=>>>")
    orderDataStreamJson.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order`))
  }
}
