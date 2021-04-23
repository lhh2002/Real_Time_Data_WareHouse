package com.canal.canal_clinet.kafka;

import com.canal.bean.CanalRowData;
import com.canal.canal_clinet.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author 大数据老哥
 * @version V1.0
 * @Package com.canal.canal_clinet.kafka
 * @File ：KafkaSender.java
 * @date 2021/3/13 23:00
 */
public class KafkaSender {

    private Properties kafkaProp =new Properties();;
    private KafkaProducer<String, CanalRowData> kafkaProducer;


    /**
     *  kafka初始化配置
     */
    public KafkaSender() {
        //kafka集群地址
        kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.kafkaBootstrap_servers_config());
        //配置批次发送数据的大小，满足批次大小才会发送数据
        kafkaProp.put(ProducerConfig.BATCH_SIZE_CONFIG, ConfigUtil.kafkaBatch_size_config());
        // 配置ack
        kafkaProp.put(ProducerConfig.ACKS_CONFIG, ConfigUtil.kafkaAcks());
        kafkaProp.put(ProducerConfig.RETRIES_CONFIG, ConfigUtil.kafkaRetries());
        kafkaProp.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigUtil.kafkaClient_id_config());
        kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaKey_serializer_class_config());
        kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaValue_serializer_class_config());
        kafkaProducer = new KafkaProducer<>(kafkaProp);
    }
    // 传递参数，将数据写入kafka集群
    public void send(CanalRowData rowData){
        kafkaProducer.send(new ProducerRecord<>(ConfigUtil.kafkaTopic(), rowData));
    }
}
