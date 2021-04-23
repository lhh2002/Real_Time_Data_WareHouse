package cn.shop.realtime.etl.process

/**
 * @Package cn.shop.realtime.etl.process
 * @File ：CommentsDataETL.java
 * @author 大数据老哥
 * @date 2021/4/5 13:30
 * @version V1.0
 */
import java.sql.{Date, Timestamp}

import cn.shop.realtime.etl.`trait`.MQBaseETL
import cn.shop.realtime.etl.bean.{CommentsEntity, CommentsWideEntity, DimGoodsDBEntity}
import cn.shop.realtime.etl.utils.{DateUtil, GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.Jedis

/**
 * 评论数据的实时ETL处理
 * @param env
 */
case class CommentsDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    // 1. 整合Kafka
    val commentsDS: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.comments`)

    commentsDS.printToErr("消费出来的评论数据>>>")
    // 2. Flink实时ETL
    // 将JSON转换为实体类
    val commentsBeanDS: DataStream[CommentsEntity] = commentsDS.map(
      commentsJson=>{
        CommentsEntity(commentsJson)
      }
    )

    commentsBeanDS.printToErr("评论信息>>>")
    //将评论信息表进行拉宽操作
    val commentsWideBeanDataStream: DataStream[CommentsWideEntity] = commentsBeanDS.map(new RichMapFunction[CommentsEntity, CommentsWideEntity] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      //释放资源
      override def close(): Unit = {
        if (jedis !=null && jedis.isConnected) {
          jedis.close()
        }
      }

      //对数据进行拉宽
      override def map(comments: CommentsEntity): CommentsWideEntity = {
        // 拉宽商品
        println("goodsJson start..."+comments.goodsId)
        val goodsJSON = jedis.hget("itcast_shop:dim_goods", comments.goodsId)
        println("goodsJson"+goodsJSON)
        val dimGoods = DimGoodsDBEntity(goodsJSON)

        //将时间戳转换为时间类型
        val timestamp = new Timestamp(comments.timestamp)
        val date = new Date(timestamp.getTime)

        CommentsWideEntity(
          comments.userId,
          comments.userName,
          comments.orderGoodsId,
          comments.starScore,
          comments.comments,
          comments.assetsViedoJSON,
          DateUtil.date2DateStr(date, "yyyy-MM-dd HH:mm:ss"),
          comments.goodsId,
          dimGoods.goodsName,
          dimGoods.shopId
        )
      }
    })

    //3:将评论信息数据拉宽处理
    val commentsJsonDataStream: DataStream[String] = commentsWideBeanDataStream.map(commentsWideEntity => {
      JSON.toJSONString(commentsWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    commentsJsonDataStream.printToErr("拉宽后评论信息>>>")

    //4：将关联维度表后的数据写入到kafka中，供Druid进行指标分析
    commentsJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.comments`))
  }
}