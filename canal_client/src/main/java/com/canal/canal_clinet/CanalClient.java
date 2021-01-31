package com.canal.canal_clinet;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.canal.canal_clinet.util.ConfigUtil;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author 大数据老哥
 * @version V1.0
 * @Package com.canal.canal_clinet
 * @File ：CanalClient.java
 * @date 2021/1/31 14:32
 * canal 客户端
 */
public class CanalClient {
    // 一次读取binlog数据的条数
    private static final int BATCH_SIZE = 5 * 1024;
    // canal 客户端连接器
    private CanalConnector canalConnector;
    //canal 配置项
    private Properties properties;
//    private KafkaSender kafkaSender;

    // 初始化连接
    public CanalClient() {
        canalConnector = CanalConnectors.newClusterConnector(ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalServerDestination(),
                ConfigUtil.canalServerIp(),
                ConfigUtil.canalServerUsername());
    }

    //启动方法
    public void start() {
        try {
            // 建立连接
            canalConnector.connect();
            // 回滚上一次请求，重新获取数据
            canalConnector.rollback();
            // 订阅拉去binlog日志，一次性拉去多条数据
            canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());
            while (true) {
                Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                // 获取batchid
                long batchId = message.getId();
                // 获取binlog 条数
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {
                    //没有拉去到数据
                } else {
                    Map binlogMessageToMap = binlogMessageToMap(message);
                    // 将map 对象进行序列化对
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            canalConnector.disconnect();
        }
    }

    /**
     * 将binlog日志转换为Map结构
     *
     * @param message
     * @return
     */
    private Map binlogMessageToMap(Message message) throws InvalidProtocolBufferException {
        Map rowDataMap = new HashMap();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            // 获取所有行上的变更
            Map<String, String> columnDataMap = new HashMap<>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
            }

            rowDataMap.put("columns", columnDataMap);
        }

        return rowDataMap;
    }
}
