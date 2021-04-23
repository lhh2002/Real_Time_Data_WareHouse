package com.canal.bean;

import cn.canal.protobuf.CanalModel;
import com.canal.protobuf.ProtoBufable;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import sun.rmi.runtime.Log;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 大数据老哥
 * @version V1.0
 * @Package com.canal.bean
 * @File ：CanalRowData.java
 * @date 2021/3/13 14:53
 */
@Getter
@Setter
@ToString
public class CanalRowData implements ProtoBufable {
    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    public CanalRowData(Map map) {
        if (map.size() > 0) {
            this.logfileName = map.get("logfileName").toString();
            this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>) map.get("columns");
        }
    }


    public CanalRowData(byte[] bytes) {
        try {
            // 将字节码数据反序列成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime=rowData.getExecuteTime();
            this.schemaName=rowData.getSchemaName();
            this.tableName=rowData.getTableName();
            this.eventType=rowData.getEventType();
            this.columns=new HashMap<>();
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将解析出来的数据转为protobuf 对象然后转为字节码返回
     *
     * @return
     */
    @Override
    public byte[] toBytes() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.getLogfileName());
        builder.setLogfileOffset(this.getLogfileOffset());
        builder.setExecuteTime(this.getExecuteTime());
        builder.setSchemaName(this.getSchemaName());
        builder.setTableName(this.getTableName());
        builder.setEventType(this.getEventType());
        builder.putAllColumns(this.getColumns());
        return builder.build().toByteArray();
    }

}
