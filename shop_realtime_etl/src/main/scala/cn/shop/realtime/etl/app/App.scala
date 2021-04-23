package cn.shop.realtime.etl.app

import cn.shop.realtime.etl.process.{CartDataETL, ClickLogDataETL, CommentsDataETL, GoodsDataETL, OrderETL, OrderGoodsEtl, SyncDimDataETL}
import cn.shop.realtime.etl.utils.GlobalConfigUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._

/**
 * @Package cn.shop.realtime.etl.app
 * @File ：App.java
 * @author 大数据老哥
 * @date 2021/3/20 15:31
 * @version V1.0
 */
object App {

  def main(args: Array[String]): Unit = {
    //初始化Flink 的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置 测试环境的并行度为1
    env.setParallelism(1)
    // 配置checkpoint
    env.enableCheckpointing(5000)
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink/checkpoint"))
    // 配置 两次最大checkpoint的最小间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 配置checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 配置 checkpoint 的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 当程序关闭，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000))

    // 使用分布式缓将ip地址资源库数据拷贝到TaskManager节点上
    env.registerCachedFile(GlobalConfigUtil.`ip.file.path`,"qqwry.day")
    // TODO

    val syncDimDataETL = new SyncDimDataETL(env)
    syncDimDataETL.process()

    // 点击流日志的实时ETL
    val clickLogDataETL = new ClickLogDataETL(env)
    clickLogDataETL.process()

    // 订单数据实时ETL
    val orderETL = new OrderETL(env)
    orderETL.process()
    // 处理订单 明细表数据
    val orderGoodsEtl = new OrderGoodsEtl(env)
    orderGoodsEtl.process()

    //5.5：商品数据的实时ETL
    val goodsDataProcess = new GoodsDataETL(env)
    goodsDataProcess.process()

    //5.6：购物车数据的实时ETL
    val cartDataProcess: CartDataETL = new CartDataETL(env)
    cartDataProcess.process()

    //5.7：评论数据的实时ETL
    val commentsDataProcess = new CommentsDataETL(env)
    commentsDataProcess.process();

    env.execute("runtime_etl")
  }
}
