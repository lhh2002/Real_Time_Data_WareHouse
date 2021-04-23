package cn.shop.realtime.etl.dataloader

import java.sql
import java.sql.{Connection, DriverManager}

import cn.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import redis.clients.jedis.Jedis

/**
 * @Package cn.shop.realtime.etl.dataloader
 * @File ：DimensionDataLoader.java
 * @author 大数据老哥
 * @date 2021/3/20 21:25
 * @version V1.0
 */
object DimensionDataLoader {
  def main(args: Array[String]): Unit = {
    //1. 设置MySQL的驱动
    Class.forName("com.mysql.jdbc.Driver")
    //2. 创建MySQL的连接
    val connection: Connection = DriverManager.getConnection(s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`, GlobalConfigUtil.`mysql.server.password`
    )
    // 3.创建 jedis连接
    val jedis = RedisUtil.getJedis()
    jedis.select(1)
    loadDimGoods(connection,jedis)
    loadDimShops(connection,jedis)
    loadDimGoodsCats(connection,jedis)
    loadDimOrg(connection,jedis)
    LoadDimShopCats(connection,jedis)
    System.exit(0)
  }

  def loadDimGoods(connect: Connection, jedis: Jedis): Unit = {
    var sql =
      """
        |select
        |goodsId,
        |shopId,
        |goodsCatId,
        |shopPrice,
        |goodsName
        |from itcast_goods
        |""".stripMargin
    val statement = connect.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val goodsId = resultSet.getLong("goodsId")
      val goodsName = resultSet.getString("goodsName")
      val shopId = resultSet.getLong("shopId")
      val goodsCatId = resultSet.getInt("goodsCatId")
      val shopPrice = resultSet.getDouble("shopPrice")
      val entity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)
      println(entity)
      jedis.hset("itcast_shop:dim_goods",goodsId.toString,JSON.toJSONString(entity,SerializerFeature.DisableCircularReferenceDetect))
    }
    resultSet.close()
    statement.close()
  }
  // 加载商铺维度数据到Redis
  // 加载商铺维度数据到Redis
  def loadDimShops(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |SELECT
        |	t1.`shopId`,
        |	t1.`areaId`,
        |	t1.`shopName`,
        |	t1.`shopCompany`
        |FROM
        |	itcast_shops t1
      """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val shopId = resultSet.getInt("shopId")
      val areaId = resultSet.getInt("areaId")
      val shopName = resultSet.getString("shopName")
      val shopCompany = resultSet.getString("shopCompany")

      val dimShop = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)
      println(dimShop)
      jedis.hset("itcast_shop:dim_shops", shopId + "", JSON.toJSONString(dimShop, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }
  def loadDimGoodsCats(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`cat_level`
                |FROM
                |	itcast_goods_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("cat_level")

      val entity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("itcast_shop:dim_goods_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }
  // 加载组织结构维度数据
  def loadDimOrg(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	orgid,
                |	parentid,
                |	orgName,
                |	orgLevel
                |FROM
                |	itcast_org
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val orgId = resultSet.getInt("orgId")
      val parentId = resultSet.getInt("parentId")
      val orgName = resultSet.getString("orgName")
      val orgLevel = resultSet.getInt("orgLevel")

      val entity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
      println(entity)
      jedis.hset("itcast_shop:dim_org", orgId + "", JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }
  }
  // 加载门店商品分类维度数据到Redis
  def LoadDimShopCats(connection: Connection, jedis: Jedis): Unit ={
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`catSort`
                |FROM
                |	itcast_shop_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("catSort")

      val entity = DimShopCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("itcast_shop:dim_shop_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }
}
