package cn.shop.realtime.etl.bean

/**
 * @Package cn.shop.realtime.etl.bean
 * @File ：CommentsEntity.java
 * @author 大数据老哥
 * @date 2021/4/5 13:29
 * @version V1.0
 */
import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
 * 评论样例类
 */
case class CommentsEntity(userId:String,    // 用户ID
                          userName:String,  // 用户名
                          orderGoodsId:String, // 订单明细ID
                          starScore:Int,    // 评分
                          comments:String,  // 评论
                          assetsViedoJSON:String, // 图片、视频JSO
                          goodsId:String, //商品id
                          timestamp:Long) // 评论时间
object CommentsEntity{
  def apply(json:String): CommentsEntity = {
    val jsonObject = JSON.parseObject(json)

    CommentsEntity(
      jsonObject.getString("userId"),
      jsonObject.getString("userName"),
      jsonObject.getString("orderGoodsId"),
      jsonObject.getInteger("starScore"),
      jsonObject.getString("comments"),
      jsonObject.getString("assetsViedoJSON"),
      jsonObject.getString("goodsId"),
      jsonObject.getLong("timestamp")
    )
  }
}

case class CommentsWideEntity(@BeanProperty userId:String,        // 用户ID
                              @BeanProperty userName:String,      // 用户名
                              @BeanProperty orderGoodsId:String,  // 订单明细ID
                              @BeanProperty starScore:Int,        // 评分
                              @BeanProperty comments:String,      // 评论
                              @BeanProperty assetsViedoJSON:String, // 图片、视频JSO
                              @BeanProperty createTime:String,     // 评论时间
                              @BeanProperty goodsId:String,       // 商品id
                              @BeanProperty goodsName:String,   //商品名称，
                              @BeanProperty shopId:Long         //商家id    ---扩宽后的字段
                             )