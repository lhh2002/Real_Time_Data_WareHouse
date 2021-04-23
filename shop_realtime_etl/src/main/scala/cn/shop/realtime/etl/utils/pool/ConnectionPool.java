package cn.shop.realtime.etl.utils.pool;

import java.io.Serializable;

public interface ConnectionPool<T> extends Serializable {

    /**
     * <p>Title: getConnection</p>
     * <p>Description: 获取连接</p>
     *
     * @return 连接
     */
    public abstract T getConnection();

    /**
     * <p>Title: returnConnection</p>
     * <p>Description: 返回连接</p>
     *
     * @param conn 连接
     */
    public void returnConnection(T conn);

}
