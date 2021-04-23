package cn.shop.realtime.etl.utils.pool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Properties;

public class HbaseConnectionPool extends ConnectionPoolBase<Connection> implements ConnectionPool<Connection> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -9126420905798370243L;

    /**
     * <p>Title: HbaseConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig          池配置
     * @param hadoopConfiguration hbase配置
     */
    public HbaseConnectionPool(final ConnectionPoolConfig poolConfig, final Configuration hadoopConfiguration) {
        super(poolConfig, new HbaseConnectionFactory(hadoopConfiguration));
    }

    /**
     * @param poolConfig 池配置
     * @param properties 参数配置
     * @since 1.2.1
     */
    public HbaseConnectionPool(final ConnectionPoolConfig poolConfig, final Properties properties) {

        super(poolConfig, new HbaseConnectionFactory(properties));
    }

    @Override
    public Connection getConnection() {

        return super.getResource();
    }

    @Override
    public void returnConnection(Connection conn) {

        super.returnResource(conn);
    }
}
