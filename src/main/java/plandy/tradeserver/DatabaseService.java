package plandy.tradeserver;

import plandy.tradeserver.database.sqlite.ProcedureDefinitions;
import plandy.tradeserver.database.sqlite.Tables;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseService {

    public static void createTables( Connection p_connection ) throws SQLException {
        Statement statement = p_connection.createStatement();

        statement.execute( Tables.DROP_LISTEDSTOCKS );
        statement.execute( Tables.CREATE_LISTEDSTOCKS );

        statement.execute( Tables.DROP_PRICEHISTORY );
        statement.execute( Tables.CREATE_PRICEHISTORY );

        statement.execute( Tables.DROP_DATAREQUESTHISTORY );
        statement.execute( Tables.CREATE_DATAREQUESTHISTORY );
    }

    public static void insertInitialData( Connection p_connection ) throws SQLException {
        List<Stock> stocklist = PriceHistoryService.getAllStocks();
        PreparedStatement prepstatement = p_connection.prepareStatement( ProcedureDefinitions.I_LISTEDSTOCKS );
        for ( Stock stock : stocklist ) {
            prepstatement.setString( 1, stock.getTicker() );
            prepstatement.setString( 2, stock.getFullName() );
            prepstatement.addBatch();
        }
        int[] results = prepstatement.executeBatch();
    }

    public static void initialiseDatabase( Connection p_connection ) throws SQLException {
        try {
            createTables( p_connection );
            insertInitialData( p_connection );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static boolean isDatabaseInitialized( Connection p_connection ) throws SQLException {
        boolean isInitialized = isTableExists( Tables.TABLE_LISTEDSTOCKS, p_connection );

        return isInitialized;
    }

    public static boolean isTableExists( String p_tableName, Connection p_connection ) throws SQLException {
        boolean isExists = false;

        PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.F_IS_TABLE_EXISTS );

        preparedStatement.setString(1, p_tableName );

        ResultSet result = preparedStatement.executeQuery();
        while ( result.next() ) {
            isExists = result.getBoolean(1);
        }

        return isExists;
    }

}
