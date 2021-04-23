package cn.shop.realtime.etl.utils.pool;

public class ConnectionException extends RuntimeException {

    private static final long serialVersionUID = -6503525110247209484L;

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
