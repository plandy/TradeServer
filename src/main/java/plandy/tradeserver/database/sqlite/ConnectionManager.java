package plandy.tradeserver.database.sqlite;

public enum ConnectionManager {

    INSTANCE;

    private static final int POOL_CAPACITY = 1;

    private final ConnectionPool connectionPool = new ConnectionPool( POOL_CAPACITY );

    public PoolableConnection getConnection() {
        return connectionPool.getConnectionSpinWait();
    }

}
