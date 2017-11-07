package plandy.tradeserver;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;

public class Server extends Thread {

    private final DataService dataService;

    public Server() {
        dataService = new DataService();
    }

    @Override
    public void run() {
        ZContext zContext = new ZContext();
        ZMQ.Socket socket = zContext.createSocket(ZMQ.ROUTER);
        socket.setRouterMandatory(true);
        socket.bind("tcp://localhost:5057");

        ArrayBlockingQueue<ZMsg> outBuffer = new ArrayBlockingQueue(100);

        ZMQ.Poller zPoller = zContext.createPoller(1);
        zPoller.register( socket, ZMQ.Poller.POLLIN );

        boolean isRunning = false;

        isRunning = true;

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

                String[] requestType = new String( requestTypeFrame.getData() ).split(";");

                ZMsg reply = new ZMsg();
                reply.add( addressFrame );
                reply.add( clientRequestIDFrame );

                if ( RequestType.PRICE_HISTORY.name().equals(requestType[0]) ) {

                    String ticker = requestType[1];

                    String response = dataService.requestPriceHistoryData( ticker );

                    reply.add( new ZFrame(RequestType.PRICE_HISTORY_RESULT.name()) );
                    reply.add( new ZFrame(response) );

                } else if ( RequestType.LIST_TICKERS.name().equals(requestType[0]) ) {

                    String response = dataService.requestListTickers();

                    reply.add( new ZFrame(RequestType.LIST_TICKERS_RESULT.name()) );
                    reply.add( new ZFrame(response) );
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
