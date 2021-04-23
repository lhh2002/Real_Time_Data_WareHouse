package cn.shop.realtime.etl.process

import cn.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.shop.realtime.etl.async.Antidisestablishmentarianism
import cn.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @Package cn.shop.realtime.etl.process
 * @File ：OrderGoodsEtl.java
 * @author 大数据老哥
 * @date 2021/4/4 18:19
 * @version V1.0
 */
class OrderGoodsEtl(env:StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
   * 根据业务抽取出process方法，因为所有的ETL都有操作方法
   *
   */
  override def process(): Unit = {
    //1.过滤出订单明细数据 itcast_order_goods 表的日志数据
    val orderGoodsStream = getKafkaDataStream().filter(_.getTableName == "itcast_order_goods")
    //2.使用异步io的方式读取redis中的数据
    val orderGoodsWideDataStream: DataStream[OrderGoodsWideEntity] = AsyncDataStream.unorderedWait(orderGoodsStream, new Antidisestablishmentarianism(), 1, TimeUnit.SECONDS, 100)
    orderGoodsWideDataStream.printToErr("订单明细数据===>>")
    // 3.将数据写入到kafka中
    val orderGoodsWideJsonDataStream = orderGoodsWideDataStream.map(orderGoodsWideJson => {
      JSON.toJSONString(orderGoodsWideJson, SerializerFeature.DisableCircularReferenceDetect)
    })
    orderGoodsWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order_detail`))
    //4.将数据写入到Hbase
    orderGoodsWideDataStream.addSink(new RichSinkFunction[OrderGoodsWideEntity] {
      var connection:Connection=_
      var table:Table=_

      override def invoke(orderGoodsWideEntity: OrderGoodsWideEntity, context: SinkFunction.Context[_]): Unit = {
        //2：构建put对象
        val rowKey = Bytes.toBytes(orderGoodsWideEntity.ogId.toString)
        val put = new Put(rowKey)
        val family = Bytes.toBytes(GlobalConfigUtil.`hbase.table.family`)

        val ogIdCol = Bytes.toBytes("ogId")
        val orderIdCol = Bytes.toBytes("orderId")
        val goodsIdCol = Bytes.toBytes("goodsId")
        val goodsNumCol = Bytes.toBytes("goodsNum")
        val goodsPriceCol = Bytes.toBytes("goodsPrice")
        val goodsNameCol = Bytes.toBytes("goodsName")
        val shopIdCol = Bytes.toBytes("shopId")
        val goodsThirdCatIdCol = Bytes.toBytes("goodsThirdCatId")
        val goodsThirdCatNameCol = Bytes.toBytes("goodsThirdCatName")
        val goodsSecondCatIdCol = Bytes.toBytes("goodsSecondCatId")
        val goodsSecondCatNameCol = Bytes.toBytes("goodsSecondCatName")
        val goodsFirstCatIdCol = Bytes.toBytes("goodsFirstCatId")
        val goodsFirstCatNameCol = Bytes.toBytes("goodsFirstCatName")
        val areaIdCol = Bytes.toBytes("areaId")
        val shopNameCol = Bytes.toBytes("shopName")
        val shopCompanyCol = Bytes.toBytes("shopCompany")
        val cityIdCol = Bytes.toBytes("cityId")
        val cityNameCol = Bytes.toBytes("cityName")
        val regionIdCol = Bytes.toBytes("regionId")
        val regionNameCol = Bytes.toBytes("regionName")

        put.addColumn(family, ogIdCol, Bytes.toBytes(orderGoodsWideEntity.ogId.toString))
        put.addColumn(family, orderIdCol, Bytes.toBytes(orderGoodsWideEntity.orderId.toString))
        put.addColumn(family, goodsIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsId.toString))
        put.addColumn(family, goodsNumCol, Bytes.toBytes(orderGoodsWideEntity.goodsNum.toString))
        put.addColumn(family, goodsPriceCol, Bytes.toBytes(orderGoodsWideEntity.goodsPrice.toString))
        put.addColumn(family, goodsNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsName.toString))
        put.addColumn(family, shopIdCol, Bytes.toBytes(orderGoodsWideEntity.shopId.toString))
        put.addColumn(family, goodsThirdCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatId.toString))
        put.addColumn(family, goodsThirdCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatName.toString))
        put.addColumn(family, goodsSecondCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatId.toString))
        put.addColumn(family, goodsSecondCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatName.toString))
        put.addColumn(family, goodsFirstCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatId.toString))
        put.addColumn(family, goodsFirstCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatName.toString))
        put.addColumn(family, areaIdCol, Bytes.toBytes(orderGoodsWideEntity.areaId.toString))
        put.addColumn(family, shopNameCol, Bytes.toBytes(orderGoodsWideEntity.shopName.toString))
        put.addColumn(family, shopCompanyCol, Bytes.toBytes(orderGoodsWideEntity.shopCompany.toString))
        put.addColumn(family, cityIdCol, Bytes.toBytes(orderGoodsWideEntity.cityId.toString))
        put.addColumn(family, cityNameCol, Bytes.toBytes(orderGoodsWideEntity.cityName.toString))
        put.addColumn(family, regionIdCol, Bytes.toBytes(orderGoodsWideEntity.regionId.toString))
        put.addColumn(family, regionNameCol, Bytes.toBytes(orderGoodsWideEntity.regionName.toString))

        //3：执行put操作
        table.put(put)
      }

      override def open(parameters: Configuration): Unit = {
        connection= HbaseUtil.getPool().getConnection
        table=connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
      }

      override def close(): Unit = {}
      if (table !=null) table.close()
//      if (!connection.isClosed) HbaseUtil.getPool().returnConnection(connection)
    })
  }
}
