package cn.shop.realtime.etl.process

/**
 * @Package cn.shop.realtime.etl.process
 * @File ：CartDataETL.java
 * @author 大数据老哥
 * @date 2021/4/5 13:28
 * @version V1.0
 */
import cn.shop.realtime.etl.`trait`.MQBaseETL
import cn.shop.realtime.etl.bean.{CartEntity, CartWideEntity, DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity}
import cn.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.canal.util.ip.IPSeeker
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.Jedis

/**
 * 购物车实时ETL处理
 * @param env
 *
 *  {"addTime":1576479746005,"count":1,"goodsId":"100106","guid":"f1eeb1d9-9ee9-4eec-88da-61f87ab0302c","ip":"123.125.71.102","userId":"100208"}
 */
class CartDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    // 1. 整合Kafka
    val cartDS: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.cart`)

    cartDS.print("购物车数据转换前>>>")
    // 2. Flink实时ETL
    // 将JSON转换为实体类
    val cartBeanDS: DataStream[CartEntity] = cartDS.map(
      cartJson => {
        CartEntity(cartJson)
      }
    )

    cartBeanDS.print("购物车数据转换后>>>")

    // 3：将购物车数据拉宽
    val cartWideBeanDS: DataStream[CartWideEntity] = cartBeanDS.map(new RichMapFunction[CartEntity, CartWideEntity] {
      var jedis: Jedis = _
      var ipSeeker: IPSeeker = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
        ipSeeker = new IPSeeker(getRuntimeContext.getDistributedCache.getFile("qqwry.dat"))
      }

      override def close(): Unit = {
        if (jedis != null && jedis.isConnected) {
          jedis.close()
        }
      }

      override def map(cartEntity: CartEntity): CartWideEntity = {
        val cartWideBean: CartWideEntity = CartWideEntity(cartEntity)

        try {
          // 拉宽商品
          val goodsJSON = jedis.hget("itcast_shop:dim_goods", cartWideBean.goodsId).toString
          val dimGoods = DimGoodsDBEntity(goodsJSON)
          // 获取商品三级分类数据
          val goodsCat3JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoods.getGoodsCatId.toString).toString
          val dimGoodsCat3 = DimGoodsCatDBEntity(goodsCat3JSON)
          // 获取商品二级分类数据
          val goodsCat2JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat3.parentId).toString
          val dimGoodsCat2 = DimGoodsCatDBEntity(goodsCat2JSON)
          // 获取商品一级分类数据
          val goodsCat1JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat2.parentId).toString
          val dimGoodsCat1 = DimGoodsCatDBEntity(goodsCat1JSON)

          // 获取商品店铺数据
          val shopJSON = jedis.hget("itcast_shop:dim_shops", dimGoods.shopId.toString).toString
          val dimShop = DimShopsDBEntity(shopJSON)

          // 获取店铺管理所属城市数据
          val orgCityJSON = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString).toString
          val dimOrgCity = DimOrgDBEntity(orgCityJSON)

          // 获取店铺管理所属省份数据
          val orgProvinceJSON = jedis.hget("itcast_shop:dim_org", dimOrgCity.parentId.toString).toString
          val dimOrgProvince = DimOrgDBEntity(orgProvinceJSON)

          // 设置商品数据
          cartWideBean.goodsPrice = dimGoods.shopPrice
          cartWideBean.goodsName = dimGoods.goodsName
          cartWideBean.goodsCat3 = dimGoodsCat3.catName
          cartWideBean.goodsCat2 = dimGoodsCat2.catName
          cartWideBean.goodsCat1 = dimGoodsCat1.catName
          cartWideBean.shopId = dimShop.shopId.toString
          cartWideBean.shopName = dimShop.shopName
          cartWideBean.shopProvinceId = dimOrgProvince.orgId.toString
          cartWideBean.shopProvinceName = dimOrgProvince.orgName
          cartWideBean.shopCityId = dimOrgCity.orgId.toString
          cartWideBean.shopCityName = dimOrgCity.orgName

          //解析IP数据
          val country = ipSeeker.getCountry(cartWideBean.ip)
          var areaArray = country.split("省");
          if (areaArray.length > 1) {
            cartWideBean.clientProvince = areaArray(0) + "省";
            cartWideBean.clientCity = areaArray(1)
          }
          else {
            areaArray = country.split("市");
            if (areaArray.length > 1) {
              cartWideBean.clientProvince = areaArray(0) + "市";
              cartWideBean.clientCity = areaArray(1)
            }
            else {
              cartWideBean.clientProvince = areaArray(0);
              cartWideBean.clientCity = ""
            }
          }

          // TODO: 拉宽时间数据, 这里会抛出异常，因为DateFormatUtils对象是非线程安全的
          // cartWideBean.year = DateFormatUtils.format(cartWideBean.addTime.toLong, "yyyy")
          // cartWideBean.month = DateFormatUtils.format(cartWideBean.addTime.toLong, "MM")
          // cartWideBean.day = DateFormatUtils.format(cartWideBean.addTime.toLong, "dd")
          // cartWideBean.hour = DateFormatUtils.format(cartWideBean.addTime.toLong, "HH")
        } catch {
          case ex => println(ex)
        }
        cartWideBean
      }
    })
    cartWideBeanDS.print("拉宽后的数据>>>")

    // 4：将cartWideBeanDS转换成json字符串返回，因为kafka中需要传入字符串类型的数据
    val cartWideJsonDataStream: DataStream[String] = cartWideBeanDS.map(cartWideEntityEntity => {
      JSON.toJSONString(cartWideEntityEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    cartWideJsonDataStream.printToErr("购物车>>>")

    //5：将关联维度表后的数据写入到kafka中，供Druid进行指标分析
    cartWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.cart`))
  }
}
