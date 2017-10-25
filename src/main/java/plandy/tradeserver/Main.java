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


    public static void main( String[] args ) {

        ArrayBlockingQueue<String> inBuffer;
        ArrayBlockingQueue<String> outBuffer;

        ZContext zContext;
         ZMQ.Socket zClientSocket;

        ZMQ.Poller zPoller;

        ZMQ.PollItem[] items;

        ZContext context = new ZContext();
        ZMQ.Socket socket = context.createSocket(ZMQ.ROUTER);
        socket.setRouterMandatory(true);
        socket.bind("tcp://localhost:8011");

        inBuffer = new ArrayBlockingQueue(100);
        outBuffer = new ArrayBlockingQueue(100);

        zContext =  new ZContext();
        zClientSocket = zContext.createSocket( ZMQ.DEALER );
        zClientSocket.bind( "tcp://localhost:5057" );

        items = new ZMQ.PollItem[] {
                new ZMQ.PollItem(zClientSocket, ZMQ.Poller.POLLIN)
        };

        zPoller = zContext.createPoller(1);
        zPoller.register( zClientSocket, ZMQ.Poller.POLLIN );

        boolean isRunning = true;
        boolean sendTestString = false;

        while( isRunning ) {
            while( outBuffer.peek() != null ) {
                String outMessage = outBuffer.poll();
                zClientSocket.send( outMessage );
            }

            int numEvents = zPoller.poll(2);

            for (int i = 0; i < numEvents; i++) {
                ZMsg zMsg = ZMsg.recvMsg( zClientSocket );
                System.out.println( zMsg.toString() + Calendar.getInstance().toInstant().toString() );
                String message = "";

                int frameIndex = 0;
                for (ZFrame zFrame : zMsg) {
                    System.out.println( "frame number : " + frameIndex + Calendar.getInstance().toInstant().toString() );
                    System.out.println( "frame message : " + new String(zFrame.getData()) + Calendar.getInstance().toInstant().toString() );
                    message += new String(zFrame.getData());
                    frameIndex++;
                }
                System.out.println( "full message : " + message.toString() + Calendar.getInstance().toInstant().toString() );

                if ( "DATA_CHART".equals(message) ) {
                    outBuffer.offer(testString);
                }
            }

            try {
                Thread.sleep( 100 );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
