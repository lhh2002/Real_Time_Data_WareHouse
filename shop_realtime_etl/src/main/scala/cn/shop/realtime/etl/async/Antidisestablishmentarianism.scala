package cn.shop.realtime.etl.async

import cn.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import cn.shop.realtime.etl.utils.RedisUtil
import com.canal.bean.CanalRowData
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, future}

/**
 * @Package cn.shop.realtime.etl.async
 * @File ：Antidisestablishmentarianism.java
 * @author 大数据老哥
 * @date 2021/4/4 18:28
 * @version V1.0
 */
class Antidisestablishmentarianism extends RichAsyncFunction[CanalRowData, OrderGoodsWideEntity] {
  var redis: Jedis = _

  // 初始化redis
  override def open(parameters: Configuration): Unit = {
    redis = RedisUtil.getJedis()
    redis.select(1)
  }

  override def close(): Unit = {
    if (redis != null) {
      redis.close()
    }
  }


  override def timeout(input: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    print("超时异常")
  }
   implicit  lazy val executor=ExecutionContext.fromExecutor(Executors.directExecutor())
  override def asyncInvoke(canalRowData: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
   future {
     if (!redis.isConnected) {
       println("重新获取redis连接")
       redis = RedisUtil.getJedis()
       redis.select(1)
     }
     //1. 根据goodIs 获取商品名称
     val goodsIdJson = redis.hget("itcast_shop:dim_goods", canalRowData.getColumns.get("goodsId"))
     val dimGoods = DimGoodsDBEntity(goodsIdJson)

     //2.根据商品表的店铺id获取店铺详情信息
     val shopJson = redis.hget("itcast_shop:dim_shops", dimGoods.getShopId.toString)
     // 将店铺的字段转换成店铺样例类
     val dimShop = DimShopsDBEntity(shopJson)

     //3. 根据商品的id获取商品的分类信息
     //3.1获取商品的三级分类信息
     val thirdCatJson = redis.hget("itcast_shop:dim_goods_cats", dimGoods.getGoodsCatId.toString)
     val dimThirdCat = DimGoodsCatDBEntity(thirdCatJson)
     // 3.2 获取商品的二级分类信息
     val secondCatJson = redis.hget("itcast_shop:dim_goods_cats", dimThirdCat.getParentId)
     val dimSecondCat = DimGoodsCatDBEntity(secondCatJson)
     // 3.3 获取商品的三级分类信息
     val firstCatJson = redis.hget("itcast_shop:dim_goods_cats", dimSecondCat.parentId)
     val dimFirstCat = DimGoodsCatDBEntity(firstCatJson)

     // 4. 根据店铺表的区域id找到组织机构数据
     //4.1 根据区域id获取城市数据
     val cityJson = redis.hget("itcast_shop:dim_org", dimShop.areaId.toString)
     val dimOrgCity = DimOrgDBEntity(cityJson)
     // 4.2 根据区域的父id获取大区数据
     val regionJson = redis.hget("itcast_shop:dim_org", dimOrgCity.parentId.toString)
     val dimOrgRegion = DimOrgDBEntity(regionJson)
     //构建订单明细宽表数据对象，返回
     val orderGoodsWideEntity = OrderGoodsWideEntity(
       canalRowData.getColumns.get("ogId").toLong,
       canalRowData.getColumns.get("orderId").toLong,
       canalRowData.getColumns.get("goodsId").toLong,
       canalRowData.getColumns.get("goodsNum").toLong,
       canalRowData.getColumns.get("goodsPrice").toDouble,
       dimGoods.goodsName,
       dimShop.shopId,
       dimThirdCat.catId.toInt,
       dimThirdCat.catName,
       dimSecondCat.catId.toInt,
       dimSecondCat.catName,
       dimFirstCat.catId.toInt,
       dimFirstCat.catName,
       dimShop.areaId,
       dimShop.shopName,
       dimShop.shopCompany,
       dimOrgCity.orgId,
       dimOrgCity.orgName,
       dimOrgRegion.orgId.toInt,
       dimOrgRegion.orgName)
     //异步请求回调
     resultFuture.complete(Array(orderGoodsWideEntity))
   }
  }
}
