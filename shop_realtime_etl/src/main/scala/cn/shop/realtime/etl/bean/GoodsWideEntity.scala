package cn.shop.realtime.etl.bean

/**
 * @Package cn.shop.realtime.etl.bean
 * @File ：GoodsWideEntity.java
 * @author 大数据老哥
 * @date 2021/4/5 13:27
 * @version V1.0
 */

import scala.beans.BeanProperty

/**
 * 商品表的样例类
 */
case class GoodsWideEntity(
                            @BeanProperty goodsId:Long,           //商品id
                            @BeanProperty goodsSn:String,         //商品编号
                            @BeanProperty productNo:String,       //商品货号
                            @BeanProperty goodsName:String,       //商品名称
                            @BeanProperty goodsImg:String,        //商品图片
                            @BeanProperty shopId:String,          //门店ID
                            @BeanProperty shopName:String,        //门店名称      -》拉宽的字段
                            @BeanProperty goodsType:String,       //货物类型
                            @BeanProperty marketPrice:String,     //市场价
                            @BeanProperty shopPrice:String,       //门店价
                            @BeanProperty warnStock:String,       //预警库存
                            @BeanProperty goodsStock:String,      //商品总库存
                            @BeanProperty goodsUnit:String,       //单位
                            @BeanProperty goodsTips:String,       //促销信息
                            @BeanProperty isSale:String,          //是否上架	0:不上架 1:上架
                            @BeanProperty isBest:String,          //是否精品	0:否 1:是
                            @BeanProperty isHot:String,           //是否热销产品	0:否 1:是
                            @BeanProperty isNew:String,           //是否新品	0:否 1:是
                            @BeanProperty isRecom:String,         //是否推荐	0:否 1:是
                            @BeanProperty goodsCatIdPath:String,  //商品分类ID路径	catId1_catId2_catId3
                            //@BeanProperty goodsCatId:String,      //goodsCatId	最后一级商品分类ID
                            @BeanProperty goodsThirdCatId:Int,        //三级商品分类ID    -》拉宽的字段
                            @BeanProperty goodsThirdCatName:String,   //三级商品分类名称  -》拉宽的字段
                            @BeanProperty goodsSecondCatId:Int,       //二级商品分类ID    -》拉宽的字段
                            @BeanProperty goodsSecondCatName:String,  //二级商品分类名称  -》拉宽的字段
                            @BeanProperty goodsFirstCatId:Int,        //一级商品分类ID    -》拉宽的字段
                            @BeanProperty goodsFirstCatName:String,   //一级商品分类名称  -》拉宽的字段
                            @BeanProperty shopCatId1:String,          //门店商品分类第一级ID
                            @BeanProperty shopCatName1:String,        //门店商品分类第一级名称 -》拉宽的字段
                            @BeanProperty shopCatId2:String,          //门店商品第二级分类ID
                            @BeanProperty shopCatName2:String,        //门店商品第二级分类名称 -》拉宽的字段
                            @BeanProperty brandId:String,         //品牌ID
                            @BeanProperty goodsDesc:String,       //商品描述
                            @BeanProperty goodsStatus:String,     //商品状态	-1:违规 0:未审核 1:已审核
                            @BeanProperty saleNum:String,         //总销售量
                            @BeanProperty saleTime:String,        //上架时间
                            @BeanProperty visitNum:String,        //访问数
                            @BeanProperty appraiseNum:String,     //评价书
                            @BeanProperty isSpec:String,          //是否有规格	0:没有 1:有
                            @BeanProperty gallery:String,         //商品相册
                            @BeanProperty goodsSeoKeywords:String,  //商品SEO关键字
                            @BeanProperty illegalRemarks:String,  //状态说明	一般用于说明拒绝原因
                            @BeanProperty dataFlag:String,        //	删除标志	-1:删除 1:有效
                            @BeanProperty createTime:String,
                            @BeanProperty isFreeShipping:String,
                            @BeanProperty goodsSerachKeywords:String,
                            @BeanProperty modifyTime:String,      //修改时间
                            @BeanProperty cityId:Int,
                            @BeanProperty cityName:String,
                            @BeanProperty regionId:Int,
                            @BeanProperty regionName:String)
