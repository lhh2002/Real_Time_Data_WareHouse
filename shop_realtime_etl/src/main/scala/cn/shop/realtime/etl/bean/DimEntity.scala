package cn.shop.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.beans.BeanProperty


/**
 * 定义维度表的样例类
 *
 * @BeanProperty：生成set和get方法
 */
// 商品维度样例类
case class DimGoodsDBEntity(@BeanProperty goodsId:Long = 0,		// 商品id
                            @BeanProperty goodsName:String = "",	// 商品名称
                            @BeanProperty shopId:Long = 0,			// 店铺id
                            @BeanProperty goodsCatId:Int = 0,   // 商品分类id
                            @BeanProperty shopPrice:Double = 0) // 商品价格
/**
 * 商品的伴生对象
 */
object DimGoodsDBEntity{
  def apply(json:String): DimGoodsDBEntity = {
    //正常的话，订单明细表的商品id会存在与商品表中，假如商品id不存在商品表，这里解析的时候就会抛出异常
    if(json != null){
      val jsonObject: JSONObject = JSON.parseObject(json)
      new DimGoodsDBEntity(
        jsonObject.getLong("goodsId"),
        jsonObject.getString("goodsName"),
        jsonObject.getLong("shopId"),
        jsonObject.getInteger("goodsCatId"),
        jsonObject.getDouble("shopPrice"))
    }else{
      new DimGoodsDBEntity
    }
  }
}


// 商品分类维度样例类
case class DimGoodsCatDBEntity(@BeanProperty catId:String = "",	    // 商品分类id
                               @BeanProperty parentId:String = "",	// 商品分类父id
                               @BeanProperty catName:String = "",	  // 商品分类名称
                               @BeanProperty cat_level:String = "")	// 商品分类级别

object DimGoodsCatDBEntity {
  def apply(json:String): DimGoodsCatDBEntity = {
    if(json != null) {
      val jsonObj = JSON.parseObject(json)

      val catId = jsonObj.getString("catId")
      val catName = jsonObj.getString("catName")
      val cat_level = jsonObj.getString("cat_level")
      val parentId = jsonObj.getString("parentId")
      DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
    }else{
      new DimGoodsCatDBEntity
    }
  }
}

// 店铺维度样例类
case class DimShopsDBEntity(@BeanProperty shopId:Int  = 0,		      // 店铺id
                            @BeanProperty areaId:Int  = 0,		      // 店铺所属区域id
                            @BeanProperty shopName:String  = "",	  // 店铺名称
                            @BeanProperty shopCompany:String  = "")	// 公司名称

object DimShopsDBEntity {
  def apply(json:String): DimShopsDBEntity = {
    if(json != null) {
      val jsonObject = JSON.parseObject(json)
      val areaId = jsonObject.getString("areaId")
      val shopCompany = jsonObject.getString("shopCompany")
      val shopId = jsonObject.getString("shopId")
      val shopName = jsonObject.getString("shopName")

      DimShopsDBEntity(shopId.toInt, areaId.toInt, shopName, shopCompany)
    }else{
      new DimShopsDBEntity
    }
  }
}

// 组织结构维度样例类
case class DimOrgDBEntity(@BeanProperty orgId:Int = 0,			  // 机构id
                          @BeanProperty parentId:Int = 0,		  // 机构父id
                          @BeanProperty orgName:String = "",	// 组织机构名称
                          @BeanProperty orgLevel:Int = 0)		  // 组织机构级别

object DimOrgDBEntity {
  def apply(json:String): DimOrgDBEntity = {
    if(json != null) {
      val jsonObject = JSON.parseObject(json)
      val orgId = jsonObject.getString("orgId")
      val orgLevel = jsonObject.getString("orgLevel")
      val orgName = jsonObject.getString("orgName")
      val parentId = jsonObject.getString("parentId")

      DimOrgDBEntity(orgId.toInt, parentId.toInt, orgName, orgLevel.toInt)
    }else{
      new DimOrgDBEntity()
    }
  }
}

// 门店商品分类维度样例类
case class DimShopCatDBEntity(@BeanProperty catId:String = "",	      // 商品分类id
                              @BeanProperty parentId:String = "",	  // 商品分类父id
                              @BeanProperty catName:String = "", 	  // 商品分类名称
                              @BeanProperty catSort:String = "")	    // 商品分类级别

object DimShopCatDBEntity {
  def apply(json:String): DimShopCatDBEntity = {
    if(json != null) {
      val jsonObj = JSON.parseObject(json)
      val catId = jsonObj.getString("catId")
      val catName = jsonObj.getString("catName")
      val catSort = jsonObj.getString("catSort")
      val parentId = jsonObj.getString("parentId")
      DimShopCatDBEntity(catId, parentId, catName, catSort)
    }else{
      new DimShopCatDBEntity()
    }
  }
}