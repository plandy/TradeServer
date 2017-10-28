package plandy.tradeserver;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;

public class Main {


    private static final String testString =  "timestamp,open,high,low,close,volume\n" +
            "2017-09-21 11:00:00,74.1450,74.5100,74.1100,74.5100,541380\n" +
            "2017-09-21 10:45:00,74.3600,74.4250,74.1200,74.1450,718199\n" +
            "2017-09-21 10:30:00,74.7400,74.7600,74.2800,74.3600,793038\n" +
            "2017-09-21 10:15:00,74.7850,74.8400,74.6000,74.7400,552516\n" +
            "2017-09-21 10:00:00,75.0100,75.1000,74.7600,74.7900,719967";

    private static final String testTickers =  "ticker,fullname\n" +
            "AAPL,Apple Inc.\n" +
            "MSFT,Microsoft Corporation\n";

    public static void main( String[] args ) {

        ArrayBlockingQueue<String> inBuffer;
        ArrayBlockingQueue<ZMsg> outBuffer;

        ZContext zContext;

        ZMQ.Poller zPoller;

        ZMQ.PollItem[] items;

        zContext = new ZContext();
        ZMQ.Socket socket = zContext.createSocket(ZMQ.ROUTER);
        socket.setRouterMandatory(true);
        socket.bind("tcp://localhost:5057");

        inBuffer = new ArrayBlockingQueue(100);
        outBuffer = new ArrayBlockingQueue(100);

        zPoller = zContext.createPoller(1);
        zPoller.register( socket, ZMQ.Poller.POLLIN );

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

                zMsg.getFirst();
                ZFrame zAddressFrame = zMsg.poll();
                ZFrame clientRequestIDFrame = zMsg.poll();
                ZFrame requestTypeFrame = zMsg.poll();

                ZMsg reply = new ZMsg();
                reply.add( zAddressFrame );
                reply.add( clientRequestIDFrame );
                reply.add( new ZFrame(RequestType.LIST_TICKERS_RESULT.name()) );
                reply.add( new ZFrame(testTickers) );

                outBuffer.offer(reply);

//                if ( RequestType.PRICE_HISTORY.name().equals(message) ) {
//                    outBuffer.offer(testString);
//                } else if ( RequestType.LIST_TICKERS.name().equals(message) ) {
//                    outBuffer.offer(testTickers);
//                }
            }

            try {
                Thread.sleep( 100 );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
