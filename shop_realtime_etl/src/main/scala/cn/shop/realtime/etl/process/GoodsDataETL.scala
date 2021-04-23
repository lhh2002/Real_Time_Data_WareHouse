package cn.shop.realtime.etl.process





import cn.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity, GoodsWideEntity}
import cn.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.canal.bean.CanalRowData
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis
import org.apache.flink.api.scala._

/**
 * 商品数据的实时ETL操作
 * @param env
 */
case class GoodsDataETL(env: StreamExecutionEnvironment)  extends  MysqlBaseETL(env){
  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    // 1. 只过滤出来 itcast_goods 表的日志，并进行转换
    val goodsCanalDS: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_goods")

    // 2. 使用同步IO方式请求Redis拉取维度数据
    val goodsEntityDataStream: DataStream[GoodsWideEntity] = goodsCanalDS.map(new RichMapFunction[CanalRowData, GoodsWideEntity] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      override def map(rowData: CanalRowData): GoodsWideEntity = {
        val shopJSON = jedis.hget("itcast_shop:dim_shops", rowData.getColumns.get("shopId") + "")
        val dimShop = DimShopsDBEntity(shopJSON)

        val thirdCatJSON = jedis.hget("itcast_shop:dim_goods_cats", rowData.getColumns.get("goodsCatId") + "")
        val dimThirdCat = DimGoodsCatDBEntity(thirdCatJSON)

        val secondCatJSON = jedis.hget("itcast_shop:dim_goods_cats", dimThirdCat.parentId)
        val dimSecondCat = DimGoodsCatDBEntity(secondCatJSON)

        val firstCatJSON = jedis.hget("itcast_shop:dim_goods_cats", dimSecondCat.parentId)
        val dimFirstCat = DimGoodsCatDBEntity(firstCatJSON)

        val secondShopCatJson = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId1"))
        val dimSecondShopCat = DimShopCatDBEntity(secondShopCatJson)

        val firstShopCatJson = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId2"))
        val dimFirstShopCat = DimShopCatDBEntity(firstShopCatJson)

        val cityJSON = jedis.hget("itcast_shop:dim_org", dimShop.areaId + "")
        val dimOrgCity = DimOrgDBEntity(cityJSON)

        val regionJSON = jedis.hget("itcast_shop:dim_org", dimOrgCity.parentId + "")
        val dimOrgRegion = DimOrgDBEntity(regionJSON)

        GoodsWideEntity(rowData.getColumns.get("goodsId").toLong,
          rowData.getColumns.get("goodsSn"),
          rowData.getColumns.get("productNo"),
          rowData.getColumns.get("goodsName"),
          rowData.getColumns.get("goodsImg"),
          rowData.getColumns.get("shopId"),
          dimShop.shopName,
          rowData.getColumns.get("goodsType"),
          rowData.getColumns.get("marketPrice"),
          rowData.getColumns.get("shopPrice"),
          rowData.getColumns.get("warnStock"),
          rowData.getColumns.get("goodsStock"),
          rowData.getColumns.get("goodsUnit"),
          rowData.getColumns.get("goodsTips"),
          rowData.getColumns.get("isSale"),
          rowData.getColumns.get("isBest"),
          rowData.getColumns.get("isHot"),
          rowData.getColumns.get("isNew"),
          rowData.getColumns.get("isRecom"),
          rowData.getColumns.get("goodsCatIdPath"),
          dimThirdCat.catId.toInt,
          dimThirdCat.catName,
          dimSecondCat.catId.toInt,
          dimSecondCat.catName,
          dimFirstCat.catId.toInt,
          dimFirstCat.catName,
          dimFirstShopCat.getCatId,
          dimFirstShopCat.catName,
          dimSecondShopCat.getCatId,
          dimSecondShopCat.catName,
          rowData.getColumns.get("brandId"),
          rowData.getColumns.get("goodsDesc"),
          rowData.getColumns.get("goodsStatus"),
          rowData.getColumns.get("saleNum"),
          rowData.getColumns.get("saleTime"),
          rowData.getColumns.get("visitNum"),
          rowData.getColumns.get("appraiseNum"),
          rowData.getColumns.get("isSpec"),
          rowData.getColumns.get("gallery"),
          rowData.getColumns.get("goodsSeoKeywords"),
          rowData.getColumns.get("illegalRemarks"),
          rowData.getColumns.get("dataFlag"),
          rowData.getColumns.get("createTime"),
          rowData.getColumns.get("isFreeShipping"),
          rowData.getColumns.get("goodsSerachKeywords"),
          rowData.getColumns.get("modifyTime"),
          dimOrgCity.orgId,
          dimOrgCity.orgName,
          dimOrgRegion.orgId,
          dimOrgRegion.orgName)
      }

      override def close(): Unit = {
        if(jedis.isConnected){
          jedis.close()
        }
      }
    })

    goodsEntityDataStream.printToErr("商品数据>>>")

    //3：将商品数据转换成json字符串
    val goodsWideJsonDataStream: DataStream[String] = goodsEntityDataStream.map(goodsWideEntity => {
      JSON.toJSONString(goodsWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    //4：将商品数据写入到kafka集群
    goodsWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.goods`))
  }
}
