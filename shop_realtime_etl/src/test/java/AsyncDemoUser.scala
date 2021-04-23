import cn.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.actors.Futures.future
import scala.concurrent.ExecutionContext


/**
 * @Package
 * @File ：AsyncDemoUser.java
 * @author 大数据老哥
 * @date 2021/4/6 21:11
 * @version V1.0
 */
class AsyncDemoUser extends RichAsyncFunction[(String, String), user] {

  //初始化redis
  var redis:Jedis=_
  override def open(parameters: Configuration): Unit = {
     redis=RedisUtil.getJedis()
    redis.select(2)

  }
  implicit  lazy val executor=ExecutionContext.fromExecutor(Executors.directExecutor())
  override def asyncInvoke(input:  (String, String), resultFuture: ResultFuture[user]): Unit = {
    future {
      val user = redis.hget("demo:user", input._1)
      var users=new user(input._1,user,input._2)
       resultFuture.complete(Array(users))
    }

  }
}
