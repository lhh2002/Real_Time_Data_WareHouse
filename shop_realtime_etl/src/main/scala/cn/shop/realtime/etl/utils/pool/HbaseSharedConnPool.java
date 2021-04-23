package cn.shop.realtime.etl.utils.pool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class HbaseSharedConnPool implements ConnectionPool<Connection> {

	private static final long serialVersionUID = 1L;

	private static final AtomicReference<HbaseSharedConnPool> pool = new AtomicReference<HbaseSharedConnPool>();

    private final Connection connection;

    private HbaseSharedConnPool(Configuration configuration) throws IOException {
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    /**
     * Gets instance.
     *
     * @param properties the properties
     * @return the instance
     */
    public synchronized static HbaseSharedConnPool getInstance(final Properties properties) {
        Configuration configuration = new Configuration();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {

            configuration.set((String) entry.getKey(), (String) entry.getValue());
        }
        return getInstance(configuration);
    }

    /**
     * Gets instance.
     *
     * @param configuration the configuration
     * @return the instance
     */
    public synchronized static HbaseSharedConnPool getInstance(final Configuration configuration) {
        if (pool.get() == null)
            try {
                pool.set(new HbaseSharedConnPool(configuration));

            } catch (IOException e) {

                e.printStackTrace();
            }

        return pool.get();
    }

    @Override
    public Connection getConnection() {

        return connection;
    }

    @Override
    public void returnConnection(Connection conn) {
    }

    /**
     * Close.
     */
    public void close() {
        try {
            connection.close();
            pool.set(null);
        } catch (IOException e) {

            e.printStackTrace();
        }
    }
}
