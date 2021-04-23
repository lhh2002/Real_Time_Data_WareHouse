package cn.shop.realtime.etl.utils

import cn.canal.protobuf.CanalModel.RowData
import com.canal.bean.CanalRowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
 * @Package cn.shop.realtime.etl.utils
 * @File ：CanalRowDataDeserialzerSchema.java
 * @author 大数据老哥
 * @date 2021/3/20 20:00
 * @version V1.0
 */
class CanalRowDataDeserialzerSchema extends AbstractDeserializationSchema[CanalRowData] {
  override def deserialize(bytes: Array[Byte]): CanalRowData = {
    new CanalRowData(bytes)
  }
}
