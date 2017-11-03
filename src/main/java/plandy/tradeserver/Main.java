package plandy.tradeserver;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class Main {

    private static final String testString = "timestamp,open,high,low,close,volume\n" +
            "2017-10-27,84.3700,86.2000,83.6100,83.8100,70877350\n" +
            "2017-10-26,79.2000,79.4200,78.7500,78.7600,29181652\n" +
            "2017-10-25,78.5800,79.1000,78.0100,78.6300,19714512\n" +
            "2017-10-24,78.9000,79.2000,78.4600,78.8600,16613928\n" +
            "2017-10-23,78.9900,79.3400,78.7600,78.8300,20479731\n" +
            "2017-10-20,78.3200,78.9700,78.2200,78.8100,22517092\n" +
            "2017-10-19,77.5700,77.9300,77.3500,77.9100,14982129\n" +
            "2017-10-18,77.6700,77.8500,77.3700,77.6100,13147124\n" +
            "2017-10-17,77.4700,77.6200,77.2500,77.5900,15953665\n" +
            "2017-10-16,77.4200,77.8100,77.3500,77.6500,12331147\n" +
            "2017-10-13,77.5900,77.8700,77.2900,77.4900,15250772\n" +
            "2017-10-12,76.4900,77.2900,76.3700,77.1200,16778148\n" +
            "2017-10-11,76.3600,76.4600,75.9500,76.4200,14780652\n" +
            "2017-10-10,76.3300,76.6300,76.1400,76.2900,13734627\n" +
            "2017-10-09,75.9700,76.5500,75.8600,76.2900,11364275\n" +
            "2017-10-06,75.6700,76.0300,75.5400,76.0000,13692791\n" +
            "2017-10-05,75.2200,76.1200,74.9600,75.9700,20656238\n" +
            "2017-10-04,74.0900,74.7200,73.7100,74.6900,13287346\n" +
            "2017-10-03,74.6700,74.8800,74.1900,74.2600,11935853\n" +
            "2017-10-02,74.7100,75.0100,74.3000,74.6100,15210338\n" +
            "2017-09-29,73.9400,74.5400,73.8800,74.4900,16700435\n" +
            "2017-09-28,73.5400,73.9700,73.3100,73.8700,10814063\n" +
            "2017-09-27,73.5500,74.1700,73.1700,73.8500,18934048\n" +
            "2017-09-26,73.6700,73.8100,72.9900,73.2600,17105469\n" +
            "2017-09-25,74.0900,74.2500,72.9200,73.2600,23502422\n" +
            "2017-09-22,73.9900,74.5100,73.8500,74.4100,13969937\n" +
            "2017-09-21,75.1100,75.2400,74.1100,74.2100,19038998\n" +
            "2017-09-20,75.3500,75.5500,74.3100,74.9400,20415084\n" +
            "2017-09-19,75.2100,75.7100,75.0100,75.4400,15606870\n" +
            "2017-09-18,75.2300,75.9700,75.0400,75.1600,22730355\n" +
            "2017-09-15,74.8300,75.3900,74.0700,75.3100,37901927\n" +
            "2017-09-14,75.0000,75.4900,74.5200,74.7700,15373384\n" +
            "2017-09-13,74.9300,75.2300,74.5500,75.2100,12998629\n" +
            "2017-09-12,74.7600,75.2400,74.3700,74.6800,14003880\n" +
            "2017-09-11,74.3100,74.9400,74.3100,74.7600,17455115\n" +
            "2017-09-08,74.3300,74.4400,73.8400,73.9800,14474383\n" +
            "2017-09-07,73.6800,74.6000,73.6000,74.3400,17165518\n" +
            "2017-09-06,73.7400,74.0400,73.3500,73.4000,15945136\n" +
            "2017-09-05,73.3400,73.8900,72.9800,73.6100,21432599\n" +
            "2017-09-01,74.7100,74.7400,73.6400,73.9400,21593192\n" +
            "2017-08-31,74.0300,74.9600,73.8000,74.7700,26688077\n" +
            "2017-08-30,73.0100,74.2100,72.8300,74.0100,16826094\n" +
            "2017-08-29,72.2500,73.1600,72.0500,73.0500,11325418\n" +
            "2017-08-28,73.0600,73.0900,72.5500,72.8300,14112777\n" +
            "2017-08-25,72.8600,73.3500,72.4800,72.8200,12574503\n" +
            "2017-08-24,72.7400,72.8600,72.0700,72.6900,15980144\n" +
            "2017-08-23,72.9600,73.1500,72.5300,72.7200,13586784\n" +
            "2017-08-22,72.3500,73.2400,72.3500,73.1600,14183146\n" +
            "2017-08-21,72.4700,72.4800,71.7000,72.1500,17656716\n" +
            "2017-08-18,72.2700,72.8400,71.9300,72.4900,18215276\n" +
            "2017-08-17,73.5800,73.8700,72.4000,72.4000,21834250\n" +
            "2017-08-16,73.3400,74.1000,73.1700,73.6500,17814317\n" +
            "2017-08-15,73.5900,73.5900,73.0400,73.2200,17791179\n" +
            "2017-08-14,73.0600,73.7200,72.9500,73.5900,19756773\n" +
            "2017-08-11,71.6100,72.7000,71.2800,72.5000,21121250\n" +
            "2017-08-10,71.9000,72.1900,71.3500,71.4100,23153711\n" +
            "2017-08-09,72.2500,72.5100,72.0500,72.4700,20401071\n" +
            "2017-08-08,72.0900,73.1300,71.7500,72.7900,21446993\n" +
            "2017-08-07,72.8000,72.9000,72.2600,72.4000,18582345\n" +
            "2017-08-04,72.4000,73.0400,72.2400,72.6800,22412719\n" +
            "2017-08-03,72.1900,72.4400,71.8500,72.1500,17937522\n" +
            "2017-08-02,72.5500,72.5600,71.4400,72.2600,26405096\n" +
            "2017-08-01,73.1000,73.4200,72.4900,72.5800,19060885\n" +
            "2017-07-31,73.3000,73.4400,72.4100,72.7000,23151962\n" +
            "2017-07-28,72.6700,73.3100,72.5400,73.0400,17472880\n" +
            "2017-07-27,73.7600,74.4200,72.3200,73.1600,35518251\n" +
            "2017-07-26,74.3400,74.3800,73.8100,74.0500,15850344\n" +
            "2017-07-25,73.8000,74.3100,73.5000,74.1900,21522189\n" +
            "2017-07-24,73.5300,73.7500,73.1300,73.6000,20836422\n" +
            "2017-07-21,73.4500,74.2900,73.1700,73.7900,45302930\n" +
            "2017-07-20,74.1800,74.3000,73.2800,74.2200,34174677\n" +
            "2017-07-19,73.5000,74.0400,73.4500,73.8600,21769229\n" +
            "2017-07-18,73.0900,73.3900,72.6600,73.3000,26150272\n" +
            "2017-07-17,72.8000,73.4500,72.7200,73.3500,21481069\n" +
            "2017-07-14,72.2400,73.2700,71.9600,72.7800,25689303\n" +
            "2017-07-13,71.5000,72.0399,71.3100,71.7700,20149208\n" +
            "2017-07-12,70.6900,71.2800,70.5500,71.1500,17382861\n" +
            "2017-07-11,70.0000,70.6800,69.7500,69.9900,16880205\n" +
            "2017-07-10,69.4600,70.2500,69.2000,69.9800,14903400\n" +
            "2017-07-07,68.7000,69.8400,68.7000,69.4600,15897154\n" +
            "2017-07-06,68.2700,68.7800,68.1200,68.5700,20776555\n" +
            "2017-07-05,68.2600,69.4400,68.2200,69.0800,20174523\n" +
            "2017-07-03,69.3300,69.6000,68.0200,68.1700,16165500\n" +
            "2017-06-30,68.7800,69.3800,68.7400,68.9300,23039328\n" +
            "2017-06-29,69.3800,69.4900,68.0900,68.4900,28231562\n" +
            "2017-06-28,69.2100,69.8400,68.7900,69.8000,25226070\n" +
            "2017-06-27,70.1100,70.1800,69.1800,69.2100,24862560\n" +
            "2017-06-26,71.4000,71.7100,70.4400,70.5300,19308122\n" +
            "2017-06-23,70.0900,71.2500,69.9200,71.2100,23176418\n" +
            "2017-06-22,70.5400,70.5900,69.7100,70.2600,22222851\n" +
            "2017-06-21,70.2100,70.6200,69.9400,70.2700,19190623\n" +
            "2017-06-20,70.8200,70.8700,69.8700,69.9100,20775590\n" +
            "2017-06-19,70.5000,70.9400,70.3500,70.8700,23146852\n" +
            "2017-06-16,69.7300,70.0300,69.2200,70.0000,46911637\n" +
            "2017-06-15,69.2700,70.2100,68.8000,69.9000,25701569\n" +
            "2017-06-14,70.9100,71.1000,69.4300,70.2700,25271276\n" +
            "2017-06-13,70.0200,70.8200,69.9600,70.6500,24815455\n" +
            "2017-06-12,69.2500,69.9400,68.1300,69.7800,47363986\n" +
            "2017-06-09,72.0400,72.0800,68.5900,70.3200,48619420\n" +
            "2017-06-08,72.5100,72.5200,71.5000,71.9500,23982410\n";

    private static final String testTickers =  "ticker,fullname\n" +
            "AAPL,Apple Inc.\n" +
            "MSFT,Microsoft Corporation\n";

    //private static final String templateRequest = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv&outputsize=compact&apikey=demo&symbol=MSFT";
    private static final String requestString = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&datatype=csv&outputsize=compact";


    public static void main( String[] args ) {

        ArrayBlockingQueue<String> inBuffer;
        ArrayBlockingQueue<ZMsg> outBuffer;

        ZContext zContext;
        ZMQ.Poller zPoller;
        zContext = new ZContext();
        ZMQ.Socket socket = zContext.createSocket(ZMQ.ROUTER);
        socket.setRouterMandatory(true);
        socket.bind("tcp://localhost:5057");

        inBuffer = new ArrayBlockingQueue(100);
        outBuffer = new ArrayBlockingQueue(100);

        zPoller = zContext.createPoller(1);
        zPoller.register( socket, ZMQ.Poller.POLLIN );

        InputStream inStream = Main.class.getResourceAsStream("/alphavantage_apikey");
        Scanner s = new Scanner(inStream).useDelimiter("\\A");;
        String apiKey = s.hasNext() ? s.next() : "";

        boolean isRunning = true;

        while( isRunning ) {
            while( outBuffer.peek() != null ) {
                ZMsg outMessage = outBuffer.poll();
                outMessage.send( socket );
            }

            int numEvents = zPoller.poll(2);

            for (int i = 0; i < numEvents; i++) {
                ZMsg zMsg = ZMsg.recvMsg( socket );
                System.out.println( zMsg.toString() + Calendar.getInstance().toInstant().toString() );

                ZFrame addressFrame = zMsg.poll();
                ZFrame clientRequestIDFrame = zMsg.poll();
                ZFrame requestTypeFrame = zMsg.poll();

                String requestType = new String( requestTypeFrame.getData() );

                ZMsg reply = new ZMsg();
                reply.add( addressFrame );
                reply.add( clientRequestIDFrame );

                if ( RequestType.PRICE_HISTORY.name().equals(requestType) ) {

                    ZFrame tickerFrame = zMsg.poll();
                    String ticker = new String( tickerFrame.getData() );

                    String request = requestString + "&apikey=" + apiKey;
                    request = request + "&symbol=" + ticker;

                    URL url = null;
                    String response = null;
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

                    reply.add( new ZFrame(RequestType.PRICE_HISTORY_RESULT.name()) );
                    reply.add( new ZFrame(response) );

                } else if ( RequestType.LIST_TICKERS.name().equals(requestType) ) {
                    reply.add( new ZFrame(RequestType.LIST_TICKERS_RESULT.name()) );
                    reply.add( new ZFrame(ListTickersConstants.tickers) );
                }

                outBuffer.offer(reply);
            }

            try {
                Thread.sleep( 100 );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
