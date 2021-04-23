package cn.shop.realtime.etl.bean

import scala.beans.BeanProperty

/**
 * @Package cn.shop.realtime.etl.bean
 * @File ：OrderGoodsWideEntity.java
 * @author 大数据老哥
 * @date 2021/4/3 20:16
 * @version V1.0
 */
// 订单明细拉宽数据
case class OrderGoodsWideEntity(@BeanProperty ogId:Long,
                                @BeanProperty orderId:Long,
                                @BeanProperty goodsId:Long,
                                @BeanProperty goodsNum:Long,
                                @BeanProperty goodsPrice:Double,
                                @BeanProperty goodsName:String,
                                @BeanProperty shopId:Long,
                                @BeanProperty goodsThirdCatId:Int,
                                @BeanProperty goodsThirdCatName:String,
                                @BeanProperty goodsSecondCatId:Int,
                                @BeanProperty goodsSecondCatName:String,
                                @BeanProperty goodsFirstCatId:Int,
                                @BeanProperty goodsFirstCatName:String,
                                @BeanProperty areaId:Int,
                                @BeanProperty shopName:String,
                                @BeanProperty shopCompany:String,
                                @BeanProperty cityId:Int,
                                @BeanProperty cityName:String,
                                @BeanProperty regionId:Int,
                                @BeanProperty regionName:String)
