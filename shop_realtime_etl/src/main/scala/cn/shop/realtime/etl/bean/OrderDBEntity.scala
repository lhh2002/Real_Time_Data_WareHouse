package cn.shop.realtime.etl.bean

import cn.canal.protobuf.CanalModel.RowData
import com.canal.bean.CanalRowData

import scala.beans.BeanProperty

/**
 * @Package cn.shop.realtime.etl.bean
 * @File ：OrderDBEntity.java
 * @author 大数据老哥
 * @date 2021/4/3 20:17
 * @version V1.0
 */
/**
 * 订单数据实体
 */
case class OrderDBEntity(@BeanProperty orderId:Long,          //订单id
                         @BeanProperty orderNo:String,        //订单编号
                         @BeanProperty userId:Long,           //用户id
                         @BeanProperty orderStatus:Int,       //订单状态 -3:用户拒收-2:未付款的订单-1：用户取消 0:待发货 1:配送中 2:用户确认收货
                         @BeanProperty goodsMoney:Double,     //商品金额
                         @BeanProperty deliverType:Int,       //收货方式0:送货上门1:自提
                         @BeanProperty deliverMoney:Double,   //运费
                         @BeanProperty totalMoney:Double,     //订单金额（包括运费）
                         @BeanProperty realTotalMoney:Double, //实际订单金额（折扣后金额）
                         @BeanProperty payType:Int,           //支付方式
                         @BeanProperty isPay:Int,             //是否支付0:未支付1:已支付
                         @BeanProperty areaId:Int,            //区域最低一级
                         @BeanProperty areaIdPath:String,     //区域idpath
                         @BeanProperty userName:String,       //收件人姓名
                         @BeanProperty userAddress:String,    //收件人地址
                         @BeanProperty userPhone:String,      //收件人电话
                         @BeanProperty orderScore:Int,        //订单所得积分
                         @BeanProperty isInvoice:Int,         //是否开发票1:需要0:不需要
                         @BeanProperty invoiceClient:String,  //发票抬头
                         @BeanProperty orderRemarks:String,   //订单备注
                         @BeanProperty orderSrc:Int,          //订单来源0:商城1:微信2:手机版3:安卓App4:苹果App
                         @BeanProperty needPay:Double,        //需缴费用
                         @BeanProperty payRand:Int,           //货币单位
                         @BeanProperty orderType:Int,         //订单类型
                         @BeanProperty isRefund:Int,          //是否退款0:否1：是
                         @BeanProperty isAppraise:Int,        //是否点评0:未点评1:已点评
                         @BeanProperty cancelReason:Int,      //取消原因ID
                         @BeanProperty rejectReason:Int,      //用户拒绝原因ID
                         @BeanProperty rejectOtherReason:String, //用户拒绝其他原因
                         @BeanProperty isClosed:Int,          //订单是否关闭
                         @BeanProperty goodsSearchKeys:String,
                         @BeanProperty orderunique:String,    //订单流水号
                         @BeanProperty receiveTime:String,    //收货时间
                         @BeanProperty deliveryTime:String,   //发货时间
                         @BeanProperty tradeNo:String,        //在线支付交易流水
                         @BeanProperty dataFlag:Int,          //订单有效标志 -1：删除 1:有效
                         @BeanProperty createTime:String,     //下单时间
                         @BeanProperty settlementId:Int,      //是否结算，大于0的话则是结算ID
                         @BeanProperty commissionFee:Double,  //订单应收佣金
                         @BeanProperty scoreMoney:Double,     //积分抵扣金额
                         @BeanProperty useScore:Int,          //花费积分
                         @BeanProperty orderCode:String,
                         @BeanProperty extraJson:String,      //额外信息
                         @BeanProperty orderCodeTargetId:Int,
                         @BeanProperty noticeDeliver:Int,     //提醒发货 0:未提醒 1:已提醒
                         @BeanProperty invoiceJson:String,    //发票信息
                         @BeanProperty lockCashMoney:Double,  //锁定提现金额
                         @BeanProperty payTime:String,        //支付时间
                         @BeanProperty isBatch:Int,           //是否拼单
                         @BeanProperty totalPayFee:Int,       //总支付金额
                         @BeanProperty isFromCart:String      //是否来自购物车 0：直接下单  1：购物车
                        )
object OrderDBEntity {
  def apply(rowData:CanalRowData): OrderDBEntity = {
    OrderDBEntity(
      rowData.getColumns.get("orderId").toLong,
      rowData.getColumns.get("orderNo"),
      rowData.getColumns.get("userId").toLong,
      rowData.getColumns.get("orderStatus").toInt,
      rowData.getColumns.get("goodsMoney").toDouble,
      rowData.getColumns.get("deliverType").toInt,
      rowData.getColumns.get("deliverMoney").toDouble,
      rowData.getColumns.get("totalMoney").toDouble,
      rowData.getColumns.get("realTotalMoney").toDouble,
      rowData.getColumns.get("payType").toInt,
      rowData.getColumns.get("isPay").toInt,
      rowData.getColumns.get("areaId").toInt,
      rowData.getColumns.get("areaIdPath"),
      rowData.getColumns.get("userName"),
      rowData.getColumns.get("userAddress"),
      rowData.getColumns.get("userPhone"),
      rowData.getColumns.get("orderScore").toInt,
      rowData.getColumns.get("isInvoice").toInt,
      rowData.getColumns.get("invoiceClient"),
      rowData.getColumns.get("orderRemarks"),
      rowData.getColumns.get("orderSrc").toInt,
      rowData.getColumns.get("needPay").toDouble,
      rowData.getColumns.get("payRand").toInt,
      rowData.getColumns.get("orderType").toInt,
      rowData.getColumns.get("isRefund").toInt,
      rowData.getColumns.get("isAppraise").toInt,
      rowData.getColumns.get("cancelReason").toInt,
      rowData.getColumns.get("rejectReason").toInt,
      rowData.getColumns.get("rejectOtherReason"),
      rowData.getColumns.get("isClosed").toInt,
      rowData.getColumns.get("goodsSearchKeys"),
      rowData.getColumns.get("orderunique"),
      rowData.getColumns.get("receiveTime"),
      rowData.getColumns.get("deliveryTime"),
      rowData.getColumns.get("tradeNo"),
      rowData.getColumns.get("dataFlag").toInt,
      rowData.getColumns.get("createTime"),
      rowData.getColumns.get("settlementId").toInt,
      rowData.getColumns.get("commissionFee").toDouble,
      rowData.getColumns.get("scoreMoney").toDouble,
      rowData.getColumns.get("useScore").toInt,
      rowData.getColumns.get("orderCode"),
      rowData.getColumns.get("extraJson"),
      rowData.getColumns.get("orderCodeTargetId").toInt,
      rowData.getColumns.get("noticeDeliver").toInt,
      rowData.getColumns.get("invoiceJson"),
      rowData.getColumns.get("lockCashMoney").toDouble,
      rowData.getColumns.get("payTime"),
      rowData.getColumns.get("isBatch").toInt,
      rowData.getColumns.get("totalPayFee").toInt,
      rowData.getColumns.get("isFromCart")
    )
  }
}