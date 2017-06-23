
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.util.*;
import java.util.concurrent.Semaphore;



public class BranchingMechanism {

    public static BranchHandler bHandler;
    public static Branch.Processor processor;
    static int portNo = 0;
    static String branchName = null;
    static Branch.Client client = null;
    static BranchID myID = new BranchID();
    static int messageID =0;
    static Semaphore mutex = new Semaphore(1);


    public static void main(String[] args){

        if (args.length == 2) {
            branchName = args[0];
            portNo = Integer.parseInt(args[1]);
            bHandler.name = branchName;
            System.out.println("branch name "+branchName+ " "+portNo);

            myID.setName(branchName);
            myID.setPort(portNo);

            try {
                bHandler = new BranchHandler();
                processor = new Branch.Processor(bHandler);

                Runnable simple = new Runnable() {
                    @Override
                    public void run() {
                        simple(processor);
                    }
                };
                new Thread(simple).start();
                while (true) {
                    Random RNumber = new Random();
                    Random Rsleep = new Random();
                   Thread.sleep(Rsleep.nextInt((15000-10000)+1)+10000);
                    if (bHandler.branchList.size()>0) {
                        final int RBranch = RNumber.nextInt((bHandler.branchList.size()));


                        TTransport transport = new TSocket(bHandler.branchList.get(RBranch).getIp(), bHandler.branchList.get(RBranch).getPort());
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        client = new Branch.Client(protocol);

                        Runnable clientSimple = new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    perform(client, RBranch);
                                } catch (Exception e) {
                                    System.err.println("Client Error. Connection failed to send money");
                                }
                            }
                        };
                        new Thread(clientSimple).start();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else{
            System.out.println("Invalid arguments");
            System.exit(1);
        }
    }

    public static void simple (Branch.Processor processor){
        try{
            TServerTransport serverTransport = new TServerSocket(portNo);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            System.out.println("Server started on "+portNo);
            server.serve();
            serverTransport.close();
        }catch (Exception e ){
            e.printStackTrace();
        }
    }

    public static void perform (Branch.Client client, int RandReceiver) {

        try {
            mutex.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        TransferMessage message = new TransferMessage();
        if (bHandler.snapidlist.size() > 0){
            while (bHandler.snapidlist.size()>0) {
                for (int i = 0; i < bHandler.branchList.size(); i++) {
                    Integer SSID = bHandler.snapidlist.get(0);
                    BranchID send2Branch = bHandler.branchList.get(i);
                        TTransport transport = new TSocket(send2Branch.getIp(), send2Branch.getPort());
                        try {
                            transport.open();
                            TProtocol protocol = new TBinaryProtocol(transport);
                            Branch.Client markerToClient = new Branch.Client(protocol);
                            int messageID = bHandler.msgSentMap.get(send2Branch.getName()) + 1;
                            //int messageID = msgIDno.get(branchList.get(i).getName())+1;
                            //msgIDno.put(branchList.get(i).getName(),messageID);
                            markerToClient.Marker(bHandler.branchId, SSID, messageID);
                            bHandler.msgSentMap.put(send2Branch.getName(), messageID);
                            //System.out.println("msgid " + messageID + " marker snapid " + SSID + " sending to " + send2Branch.getName());

                        } catch (Exception e) {
                            System.err.println("marker sending connection failed to" + send2Branch.getName());
                            //e.printStackTrace();
                            //System.exit(1);
                        }
                        transport.close();

                        bHandler.markerSendmap.put(send2Branch.getName(), 0);
                   // }
                }
                bHandler.snapidlist.remove(0);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        try {


             if (bHandler.branchList.size() > 0) {
                if (bHandler.branchBalance > 0) {
                    //Random random = new Random();
                    int amountToSend;// = random.nextInt(5)*100;
                    /*long randAmntToSend = 0;
                    randAmntToSend = Math.round((Math.random() * (bHandler.branchBalance - 0) + 0) / 100) * 100;
                    amountToSend = (int) randAmntToSend;*/

                    Random random1 = new Random();
                    int Rnumber=(random1.nextInt((5-1)+1)+1);
                    amountToSend=(int) (bHandler.branchBalance*(float)Rnumber/100);


                    messageID = bHandler.msgSentMap.get(bHandler.branchList.get(RandReceiver).getName()) + 1;
                    //messageID = bHandler.msgIDno.get(bHandler.branchList.get(RandReceiver).getName()) + 1;
                    //bHandler.msgIDno.put(bHandler.branchList.get(RandReceiver).getName(), messageID);

                    bHandler.branchBalance = bHandler.branchBalance - amountToSend;

                    System.out.println("msgid-" + messageID + " " + myID.getName() + " sending money " + amountToSend + " to " +
                            bHandler.branchList.get(RandReceiver).getName()+" total money "+ bHandler.branchBalance);

                    message.setOrig_branchId(myID);
                    message.setAmount(amountToSend);
                    try {
                        client.transferMoney(message, messageID);
                        bHandler.msgSentMap.put(bHandler.branchList.get(RandReceiver).getName(), messageID);

                    } catch (Exception e) {
                        System.err.println("Transfer money connection failed to "+bHandler.branchList.get(RandReceiver));
                        e.printStackTrace();
                    }

                }
            }
            mutex.release();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

