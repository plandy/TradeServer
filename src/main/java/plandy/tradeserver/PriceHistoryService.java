package plandy.tradeserver;

import plandy.tradeserver.database.sqlite.ProcedureDefinitions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PriceHistoryService {

    public static List<Stock> getAllStocks() {

        List<Stock> listStocks = new ArrayList<>(50);

        StringReader stringReader = new StringReader(ListTickersConstants.TICKERS);
        BufferedReader buff = new BufferedReader( stringReader );
        String line;

        try {
            line = buff.readLine();
            String[] columnPositions = line.split(",");

            int tickerIndex = -1;
            int fullnameIndex = -1;

            for ( int i = 0; i < columnPositions.length; i++ ) {
                switch( columnPositions[i] ) {
                    case "ticker": tickerIndex = i;
                    case "fullname": fullnameIndex = i;
                }

            }

            while ( (line = buff.readLine()) != null ) {
                String[] values = line.split(",");
                Stock dataObject = new Stock( values[tickerIndex], values[fullnameIndex] );

                listStocks.add( dataObject );
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return listStocks;
    }

    public static String getPriceHistory( String p_ticker, Connection p_connection ) {
        StringBuilder stringBuilder = new StringBuilder( "timestamp,open,high,low,close,volume\n" );

        try {
            PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.S_PRICEHISTORY );
            preparedStatement.setString( 1, p_ticker );
            preparedStatement.setString( 2, DateUtility.parseDateToString( DateUtility.addYears( DateUtility.getTodayDate(), -1 ) ) );

            preparedStatement.setFetchSize(5000);
            ResultSet results = preparedStatement.executeQuery();

            while ( results.next() ) {

                stringBuilder.append( results.getString("DATE") ).append(",");
                stringBuilder.append( results.getString("OPENPRICE") ).append(",");
                stringBuilder.append( results.getString("HIGHPRICE") ).append(",");
                stringBuilder.append( results.getString("LOWPRICE") ).append(",");
                stringBuilder.append( results.getString("CLOSEPRICE") ).append(",");
                stringBuilder.append( results.getString("VOLUME") );
                stringBuilder.append( "\n" );

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return stringBuilder.toString();
    }

    public static Date getMostRecentPriceHistoryDate( String p_ticker, Connection p_connection ) throws SQLException {
        Date mostRecentPriceDate = null;
        String dateString = null;

        PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.F_GET_MOSTRECENT_PRICEHISTORY_DATE );
        preparedStatement.setString( 1, p_ticker );

        ResultSet results = preparedStatement.executeQuery();

        while ( results.next() ) {
            dateString = results.getString("DATE");
        }
        if ( dateString != null ) {
            mostRecentPriceDate = DateUtility.parseStringToDate( dateString );
        }

        return mostRecentPriceDate;
    }

    public static Date getMostRecentRequestDate( String p_ticker, Connection p_connection ) throws SQLException {

        Date mostRecentDate = null;

        PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.F_GET_MOSTRECENT_DATAREQUEST_DATE );
        preparedStatement.setString( 1, p_ticker );

        ResultSet results = preparedStatement.executeQuery();

        while ( results.next() ) {
            if ( results.getString("REQUESTDATE") != null ) {
                mostRecentDate = DateUtility.parseStringToDate( results.getString("REQUESTDATE") );
            }
        }

        return mostRecentDate;
    }

    public static void insertPriceHistory( String p_ticker, String p_priceHistory, Connection p_connection ) throws SQLException {

        PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.I_PRICEHISTORY );

        StringReader stringReader = new StringReader(p_priceHistory);
        BufferedReader buff = new BufferedReader( stringReader );
        String line;

        try {
            line = buff.readLine();

            String[] columnPositions = line.split(",");

            int dateIndex = -1;
            int openIndex = -1;
            int highIndex = -1;
            int lowIndex = -1;
            int closeIndex = -1;
            int volumeIndex = -1;

            for (int i = 0; i < columnPositions.length; i++) {
                switch (columnPositions[i]) {
                    case "timestamp":
                        dateIndex = i;
                    case "open":
                        openIndex = i;
                    case "high":
                        highIndex = i;
                    case "low":
                        lowIndex = i;
                    case "close":
                        closeIndex = i;
                    case "volume":
                        volumeIndex = i;
                }

            }
            int count = 0;

            while ((line = buff.readLine()) != null) {
                System.out.println(line);

                String[] values = line.split(",");

                HashMap<String, Object> dataObject = new HashMap<String, Object>();

                preparedStatement.setString(1, p_ticker );
                preparedStatement.setString(2, values[dateIndex] );
                preparedStatement.setDouble(3, Double.parseDouble(values[openIndex]) );
                preparedStatement.setDouble(4, Double.parseDouble(values[highIndex]) );
                preparedStatement.setDouble(5, Double.parseDouble(values[lowIndex]) );
                preparedStatement.setDouble(6, Double.parseDouble(values[closeIndex]) );
                preparedStatement.setLong(7, Long.parseLong(values[volumeIndex]) );

                preparedStatement.addBatch();
                count++;

                if( count % 1000 == 0 ) {
                    int[] results = preparedStatement.executeBatch();
                }

            }

            int[] results = preparedStatement.executeBatch();

            insertDataRequestHistory( p_ticker, DateUtility.getTodayDate(), p_connection );

        } catch (IOException e) {
            throw new RuntimeException( e );
        }

    }

    public static void insertDataRequestHistory( String p_ticker, Date p_date, Connection p_connection ) throws SQLException {

        PreparedStatement preparedStatement = p_connection.prepareStatement( ProcedureDefinitions.I_DATAREQUESTHISTORY );

        preparedStatement.setString(1, p_ticker );
        preparedStatement.setString(2, DateUtility.parseDateToString(p_date) );

        preparedStatement.addBatch();

        int[] results = preparedStatement.executeBatch();
    }

}
