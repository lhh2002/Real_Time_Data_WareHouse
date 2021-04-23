package cn.shop.realtime.etl.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
 * @Package cn.shop.realtime.etl.utils
 * @File ：DataUtil.java
 * @author 大数据老哥
 * @date 2021/4/3 16:45
 * @version V1.0
 */
object DateUtil {

  def datetime2date(timeLocal:String)  ={
    val format = new SimpleDateFormat("dd/MMM/yyy:hh:mm:ss", Locale.ENGLISH)
     format.parse(timeLocal)
  }
  def date2DateStr(date:Date,format:String) ={
    val sdf = new SimpleDateFormat(format)
    sdf.format(date)
  }
}
