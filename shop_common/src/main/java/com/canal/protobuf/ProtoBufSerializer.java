package com.canal.protobuf;


import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author 大数据老哥
 * @version V1.0
 * @Package com.canal.protobuf
 * @File ：ProtoBufSerializer.java
 * @date 2021/1/31 19:18
 * 定义ProtoBuf序列化
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, ProtoBufable protoBufable) {
        return protoBufable.toBytes();
    }

    @Override
    public void close() {

    }
}
