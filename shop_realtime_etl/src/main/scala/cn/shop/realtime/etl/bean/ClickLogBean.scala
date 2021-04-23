package cn.shop.realtime.etl.bean

import nl.basjes.parse.httpdlog.{HttpdLogFormatDissector, HttpdLoglineParser}

import scala.beans.BeanProperty

/**
 * @Package cn.shop.realtime.etl.bean
 * @File ：ClickLogBean.java
 * @author 大数据老哥
 * @date 2021/3/21 21:51
 * @version V1.0
 */

class ClickLogBean {
  //用户id信息
  private[this] var _connectionClientUser: String = _

  def setConnectionClientUser(value: String): Unit = {
    _connectionClientUser = value
  }

  def getConnectionClientUser = {
    _connectionClientUser
  }

  //ip地址
  private[this] var _ip: String = _

  def setIp(value: String): Unit = {
    _ip = value
  }

  def getIp = {
    _ip
  }

  //请求时间
  private[this] var _requestTime: String = _

  def setRequestTime(value: String): Unit = {
    _requestTime = value
  }

  def getRequestTime = {
    _requestTime
  }

  //请求方式
  private[this] var _method: String = _

  def setMethod(value: String) = {
    _method = value
  }

  def getMethod = {
    _method
  }

  //请求资源
  private[this] var _resolution: String = _

  def setResolution(value: String) = {
    _resolution = value
  }

  def getResolution = {
    _resolution
  }

  //请求协议
  private[this] var _requestProtocol: String = _

  def setRequestProtocol(value: String): Unit = {
    _requestProtocol = value
  }

  def getRequestProtocol = {
    _requestProtocol
  }

  //响应码
  private[this] var _responseStatus: Int = _

  def setRequestStatus(value: Int): Unit = {
    _responseStatus = value
  }

  def getRequestStatus = {
    _responseStatus
  }

  //返回的数据流量
  private[this] var _responseBodyBytes: String = _

  def setResponseBodyBytes(value: String): Unit = {
    _responseBodyBytes = value
  }

  def getResponseBodyBytes = {
    _responseBodyBytes
  }

  //访客的来源url
  private[this] var _referer: String = _

  def setReferer(value: String): Unit = {
    _referer = value
  }

  def getReferer = {
    _referer
  }

  //客户端代理信息
  private[this] var _userAgent: String = _

  def setUserAgent(value: String): Unit = {
    _userAgent = value
  }

  def getUserAgent = {
    _userAgent
  }

  //跳转过来页面的域名:HTTP.HOST:request.referer.host
  private[this] var _referDomain: String = _

  def setReferDomain(value: String): Unit = {
    _referDomain = value
  }

  def getReferDomain = {
    _referDomain
  }

}

object ClickLogBean {
  //定义点击流日志解析规则
  val getLogFormat: String = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""

  def createClickLogParser() = {

    val parser = new HttpdLoglineParser[ClickLogBean](classOf[ClickLogBean], getLogFormat)
    parser.addTypeRemapping("request.firstline.uri.query.g", "HTTP.URI")
    parser.addTypeRemapping("request.firstline.uri.query.r", "HTTP.URI")
    parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user")
    parser.addParseTarget("setIp", "IP:connection.client.host")
    parser.addParseTarget("setRequestTime", "TIME.STAMP:request.receive.time")
    parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method")
    parser.addParseTarget("setResolution", "HTTP.URI:request.firstline.uri")
    parser.addParseTarget("setRequestProtocol", "HTTP.PROTOCOL_VERSION:request.firstline.protocol")
    parser.addParseTarget("setResponseBodyBytes", "BYTES:response.body.bytes")
    parser.addParseTarget("setReferer", "HTTP.URI:request.referer")
    parser.addParseTarget("setUserAgent", "HTTP.USERAGENT:request.user-agent")
    parser.addParseTarget("setReferDomain", "HTTP.HOST:request.referer.host")

    //返回点击流日志解析规则
    parser
  }

  //解析字符串转换成对象
  def apply(parser: HttpdLoglineParser[ClickLogBean], clickLog: String): ClickLogBean = {
    val clickLogBean = new ClickLogBean
    parser.parse(clickLogBean, clickLog)
    clickLogBean
  }

  def main(args: Array[String]): Unit = {
    val logline = "2001:980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\""
    val record = new ClickLogBean()
    val parser = createClickLogParser()
    parser.parse(record, logline)
    println(record.getConnectionClientUser)
    println(record.getIp)
    println(record.getRequestTime)
    println(record.getMethod)
    println(record.getResolution)
    println(record.getRequestProtocol)
    println(record.getResponseBodyBytes)
    println(record.getReferer)
    println(record.getUserAgent)
    println(record.getReferDomain)
  }
}

/**
 * 定义拉宽后的点击流对象
 * 因为后续需要将拉宽后的点击流对象序列化成json字符存储到kafka集群中，所以需要实现属性的set和get方法
 *
 */
case class ClickLogWideEntity(
                               @BeanProperty uid: String, // 用户id
                               @BeanProperty ip: String, // IP地址
                               @BeanProperty requestTime: String, // 访问时间
                               @BeanProperty requestMethod: String, // 请求方式
                               @BeanProperty requestUrl: String, // 请求地址
                               @BeanProperty requestProtocol: String, // 请求协议
                               @BeanProperty requestStatus: String, //响应码
                               @BeanProperty requestBodyBytes: String, // 返回的数据流量
                               @BeanProperty referer: String, //访客来源
                               @BeanProperty userAgent: String, // 用户代理信息
                               @BeanProperty refererDomain: String, // 跳转过来的域名
                               @BeanProperty var province: String, // 省份
                               @BeanProperty var city: String, //城市
                               @BeanProperty var requestDataTime: String //时间戳
                             )

object ClickLogWideEntity {
  def apply(clickLogBean: ClickLogBean): ClickLogWideEntity = {

    new ClickLogWideEntity(
      clickLogBean.getConnectionClientUser,
      clickLogBean.getIp,
      clickLogBean.getRequestTime,
      clickLogBean.getMethod,
      clickLogBean.getResolution,
      clickLogBean.getRequestProtocol,
      clickLogBean.getRequestStatus.toString,
      clickLogBean.getResponseBodyBytes,
      clickLogBean.getReferer,
      clickLogBean.getUserAgent,
      clickLogBean.getReferDomain,
      "",
      "",
      ""
    )
  }
}
