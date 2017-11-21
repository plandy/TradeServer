package plandy.tradeserver;

import plandy.tradeserver.database.sqlite.ConnectionManager;
import plandy.tradeserver.database.sqlite.PoolableConnection;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class DataService extends Thread {

    //private static final String templateRequest = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv&outputsize=compact&apikey=demo&symbol=MSFT";
    private static final String requestString = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv";
    private final String apiKey;

    private final ArrayBlockingQueue<String> requestBuffer;
    private final ArrayBlockingQueue<String> outputBuffer;
    private volatile boolean isRunning = false;

    private final PoolableConnection dbConnection;

    private final String OUTPUTSIZE_FULL = "full";
    private final String OUTPUTSIZE_COMPACT = "compact";

    public DataService() {
        requestBuffer = new ArrayBlockingQueue<String>(100);
        outputBuffer = new ArrayBlockingQueue<String>(100);

        apiKey = loadApiKey();

        dbConnection = ConnectionManager.INSTANCE.getConnection();

        initialise();
    }

    @Override
    public void run() {
        isRunning = true;

        while( isRunning ) {
            while( requestBuffer.peek() != null ) {
                String ticker = requestBuffer.poll();
                doPriceHistoryDataRequest( ticker );
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public String doPriceHistoryDataRequest( String p_ticker ) {

        String dataResult;

        dbConnection.beginTransaction();

        try {
            Date mostRecentRequestDate = PriceHistoryService.getMostRecentRequestDate( p_ticker, dbConnection );
            if ( mostRecentRequestDate == null ) {
                mostRecentRequestDate = new Date(0);
            }
            Date todayDate = DateUtility.getTodayDate();

            String dataFeedResponse;
            boolean isDataOld = DateUtility.isAfterCalendarDate(todayDate, DateUtility.addDays( mostRecentRequestDate, 80 ) );

            if ( DateUtility.isSameCalendarDate( mostRecentRequestDate, todayDate ) == false ) {

                if ( isDataOld == true ) {
                    dataFeedResponse = getFullDataFromFeed( p_ticker );
                } else {
                    dataFeedResponse = getRecentDataFromFeed( p_ticker );
                }

                PriceHistoryService.insertPriceHistory( p_ticker, dataFeedResponse ,dbConnection );
                dbConnection.commitTransaction();
            }

            dataResult = PriceHistoryService.getPriceHistory( p_ticker, dbConnection );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            dbConnection.silentRollback();
        }

        return dataResult;

    }

    private String getFullDataFromFeed( String p_ticker ) {

        String response = "";

        URL url;
        String request = buildDataRequestURL( p_ticker, true );

        Instant startTime = Instant.now();

        try {
            url = new URL( request );
            URLConnection connection = url.openConnection();
            InputStreamReader inputStream = new InputStreamReader( connection.getInputStream() );
            BufferedReader bufferedReader = new BufferedReader( inputStream );

            String line;
            while ( (line = bufferedReader.readLine()) != null ) {
                line = line + "\n";
                response = response + line;
            }

            System.out.println(response);

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }

        Instant endTime = Instant.now();

        System.out.println( "Datafeed request begin: " + startTime );
        System.out.println( "Datafeed request finish: " + endTime );

        return response;
    }

    private String getRecentDataFromFeed( String p_ticker ) {



        String response = "";

        URL url;
        String request = buildDataRequestURL( p_ticker, false );

        Instant startTime = Instant.now();

        try {
            url = new URL( request );
            URLConnection connection = url.openConnection();
            InputStreamReader inputStream = new InputStreamReader( connection.getInputStream() );
            BufferedReader bufferedReader = new BufferedReader( inputStream );

            String line;
            while ( (line = bufferedReader.readLine()) != null ) {
                line = line + "\n";
                response = response + line;
            }

            System.out.println(response);

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }

        Instant endTime = Instant.now();

        System.out.println( "Datafeed request begin: " + startTime );
        System.out.println( "Datafeed request finish: " + endTime );

        return response;
    }

    public String requestPriceHistoryData( String p_ticker ) {
        String result = doPriceHistoryDataRequest( p_ticker );
        return result;
    }

    /**
     * Quote from Alpha Vantage API doc on parameter "ouptutsize":
     * Optional: outputsize
     * By default, outputsize=compact.
     * Strings compact and full are accepted with the following specifications: compact returns only the latest 100 data points;
     * full returns the full-length time series of up to 20 years of historical data.
     *
     * @param p_ticker
     * @return
     */
    private String buildDataRequestURL( String p_ticker, boolean p_isFullHistory ) {
        String request = requestString + "&apikey=" + apiKey;

        String outputSize;
        if ( p_isFullHistory == true ) {
            outputSize = OUTPUTSIZE_FULL;
        } else {
            outputSize = OUTPUTSIZE_COMPACT;
        }
        request = request + "&outputsize=" + outputSize;

        request = request + "&symbol=" + p_ticker;

        return request;
    }

    private String loadApiKey() {
        InputStream inStream = Main.class.getResourceAsStream("/alphavantage_apikey");
        Scanner s = new Scanner(inStream).useDelimiter("\\A");;
        String apiKey = s.hasNext() ? s.next() : "";

        return apiKey;
    }

    public String requestListTickers(){
        return ListTickersConstants.TICKERS;
    }

    private void initialise() {

        try {
            boolean isDBInitialised = DatabaseService.isDatabaseInitialized( dbConnection );
            if( isDBInitialised == false ) {
                DatabaseService.initialiseDatabase( dbConnection );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
