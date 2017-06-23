
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;


public class Controller {


    public static List<BranchID> branchList = new ArrayList<>();
    static Branch.Client client = null;
    static Branch.Client snapshotClient = null;
    static int snapshotid =0;
    public static HashMap<Integer, String> listOfSnapid = new HashMap();

    public static void main (String[] args){

        int totalMoney= 0;


        if (args.length == 2){
            File file = new File(args[1]);
            totalMoney = Integer.parseInt(args[0]);
            readFile(file);
            Collections.sort(branchList);
            int branchMoney = totalMoney/branchList.size();
            System.out.println("Controller started...");

            for (int i =0; i< branchList.size(); i++){
                TTransport transport = new TSocket(branchList.get(i).getIp(), branchList.get(i).getPort());
                try {
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    client = new Branch.Client(protocol);
                    try{
                        client.initBranch(branchMoney,branchList);
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }

                }catch (Exception e){
                    System.err.println("Connection Error");
                    //System.exit(1);
                    //e.printStackTrace();
                }
                transport.close();

            }

            Runnable initSnapShot = new Runnable() {
                @Override
                public void run() {


                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int Rbranchselect;
                while (true){
                    //System.out.println("init");
                    Random Rbranch = new Random();
                    Rbranchselect = Rbranch.nextInt(branchList.size());
                    //System.out.println("init sending to "+branchList.get(Rbranchselect).getName());
                    TTransport transport = new TSocket(branchList.get(Rbranchselect).getIp(),
                            branchList.get(Rbranchselect).getPort());
                    try{
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        snapshotClient = new Branch.Client(protocol);
                        snapshotid = snapshotid+1;
                        //System.out.println("calling function");

                        snapshotClient.initSnapshot(snapshotid);
                        listOfSnapid.put(snapshotid,branchList.get(Rbranchselect).getName());
                        Thread.sleep(12000);
                        //System.out.println("function came back");
                        //System.out.println("init sleeping");

                        //System.out.println("init awake");
                    }catch (Exception e){
                        System.err.println("Initializing connection error");
                        System.exit(1);
                    }
                    transport.close();
                }



            }
            };

            new Thread(initSnapShot).start();

            Runnable retSnapShot = new Runnable() {
                @Override
                public void run() {
                    Branch.Client RetriveSnapshot;
                    int retreiveSnapNumber = 0;
                    try {
                        //Thread.sleep(20000);
                        while (true){
                            LocalSnapshot localSnapshot =null;
                            retreiveSnapNumber=retreiveSnapNumber+1;
                            Thread.sleep(15000);
                            String snapSendto = listOfSnapid.get(retreiveSnapNumber);
                            System.out.println("SnapID "+retreiveSnapNumber+" was send to "+snapSendto);
                            for (int i=0;i<branchList.size();i++){
                                TTransport transport = new TSocket(branchList.get(i).getIp(),
                                        branchList.get(i).getPort());
                                try{
                                    transport.open();
                                    TProtocol protocol = new TBinaryProtocol(transport);
                                    RetriveSnapshot = new Branch.Client(protocol);
                                    localSnapshot=RetriveSnapshot.retrieveSnapshot(retreiveSnapNumber);

                                    //System.out.println(localSnapshot.toString());
                                    System.out.print(branchList.get(i).getName()+" balance "+localSnapshot.getBalance()+" ");
                                    int j,k;
                                    for ( j=0,k=0; j<branchList.size();j++){

                                            if (branchList.get(j).getName().equals(branchList.get(i).getName())) {
                                            } else {
                                                if (localSnapshot.getMessages().get(k)!=0) {
                                                    System.out.print(branchList.get(j).getName() + "->" + branchList.get(i).getName() + " " +
                                                            localSnapshot.getMessages().get(k) + "  ");
                                                }
                                                k++;
                                            }
                                    }
                                    System.out.println();
                                }catch (Exception e){
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                                transport.close();
                            }
                            System.out.println("-------------");
                            //System.out.println("recal sleeping");

                            //System.out.println("recal awake");
                        }
                    }catch (Exception e){
                        System.err.println("Snapshot Retrieval Error");
                        System.exit(1);
                    }
                }
            };
            new Thread(retSnapShot).start();
        }
        else{
            System.out.println("Invalid Arguments");
            System.exit(1);
        }
    }

    public static void readFile (File file){
        try {
            Scanner fileScan = new Scanner(new BufferedReader(new FileReader(file)));
            while (fileScan.hasNextLine()){
                String line = fileScan.nextLine();
                Scanner lineScan = new Scanner(line);
                while (lineScan.hasNext()){
                    String branchName= lineScan.next();
                    String ip = lineScan.next();
                    int portn = Integer.parseInt(lineScan.next());
                    BranchID branchId = new BranchID();
                    branchId.setName(branchName);
                    branchId.setIp(ip);
                    branchId.setPort(portn);
                    branchList.add(branchId);
                }
            }
        }catch (Exception e){
            System.err.println("file reading error");
            //e.printStackTrace();
            System.exit(1);
        }
    }
}
