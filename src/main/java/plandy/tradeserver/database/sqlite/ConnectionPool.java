package plandy.tradeserver.database.sqlite;

import org.sqlite.SQLiteConfig;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class ConnectionPool {

    private final PoolableConnection[] pool;
    private final AtomicLong counter;
    private final long INITIAL_COUNTER_VALUE = 0L;
    private static final long EMPTY_OWNER_VALUE = -1L;
    private static final String dbFileName = "marketToolDB.db";

    public ConnectionPool( int p_capacity ) {

        File dbFile = new File( dbFileName );

        if( dbFile.exists() == false ) {
            try {
                dbFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("error creating DB file");
            }
        }

        counter = new AtomicLong( INITIAL_COUNTER_VALUE );

        pool = new PoolableConnection[p_capacity];

        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode( SQLiteConfig.JournalMode.WAL );

        for ( int i = 0; i < p_capacity; i++ ) {
            pool[i] = createPoolableConnection();
        }
    }

    public PoolableConnection getConnectionBusySpin() {

        PoolableConnection returnConnection = null;

        long ticket = counter.getAndIncrement();
        boolean success = false;

        while ( success == false ) {
            for ( PoolableConnection connection : pool ) {
                if ( connection.compareAndSet(ticket) ) {
                    returnConnection = connection;
                    success = true;
                    break;
                }
            }
        }

        return returnConnection;

    }

    public PoolableConnection getConnectionSpinWait() {

        PoolableConnection returnConnection = null;

        long ticket = counter.getAndIncrement();
        boolean success = false;

        OuterLabel:
        while ( success == false ) {
            for ( int i = 0; i < 5; i++ ) {
                for ( PoolableConnection connection : pool ) {
                    if ( connection.compareAndSet(ticket) ) {
                        returnConnection = connection;
                        break OuterLabel;
                    }
                }
            }

            LockSupport.parkNanos(1);
        }


        return returnConnection;

    }

    private PoolableConnection createPoolableConnection() {
        Connection connection;

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbFileName);
        } catch (SQLException e) {
            throw new RuntimeException("error creating database connection");
        }

        return new PoolableConnection( connection );
    }

}
