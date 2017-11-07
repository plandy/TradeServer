package plandy.tradeserver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class DataService /**extends Thread*/ {

    //private static final String templateRequest = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv&outputsize=compact&apikey=demo&symbol=MSFT";
    private static final String requestString = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv&outputsize=compact";
    private final String apiKey;

    private final ArrayBlockingQueue<String> requestBuffer;
    private volatile boolean isRunning = false;

    public DataService() {
        requestBuffer = new ArrayBlockingQueue<String>(100);

        apiKey = loadApiKey();
    }

//    @Override
//    public void run() {
//        isRunning = true;
//
//        while( isRunning ) {
//            while( requestBuffer.peek() != null ) {
//                String ticker = requestBuffer.poll();
//                doPriceHistoryDataRequest( ticker );
//            }
//
//        }
//
//    }

    private String doPriceHistoryDataRequest( String p_ticker ) {

        String response = "";

        URL url;
        String request = buildDataRequestURL( p_ticker, apiKey );

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

        return response;
    }

//    public boolean requestPriceHistoryData( String p_ticker ) {
//        boolean isSuccess = requestBuffer.offer( p_ticker );
//
//        return isSuccess;
//    }

    public String requestPriceHistoryData( String p_ticker ) {
        String result = doPriceHistoryDataRequest( p_ticker );
        return result;
    }

    private String buildDataRequestURL( String p_ticker, String p_apiKey ) {
        String request = requestString + "&apikey=" + p_apiKey;
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

}
