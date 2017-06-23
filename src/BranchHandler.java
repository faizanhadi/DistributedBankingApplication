
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class BranchHandler implements Branch.Iface{

    public static List<BranchID> branchList = new ArrayList<>();
    public static volatile HashMap<String, Integer> msgReceiveMap = new HashMap<>();
    public static volatile HashMap<String, Integer> msgSentMap = new HashMap<>();
    public static BranchID branchId = new BranchID();
    public static volatile int branchBalance = 0;
    public static String name = null;
    public static volatile HashMap<Integer, Integer> markerStatus = new HashMap<>();    //marker status snapshot, new-0, old-1
    public static volatile ConcurrentHashMap<Integer,SnapshotStatus> snapshotHashMap = new ConcurrentHashMap<>();
    public static volatile HashMap<String,Integer> markerSendmap = new HashMap<>(); //marker-0 dont send, 1 send
    public static volatile List<Integer> snapidlist = new ArrayList<>();    //snapid-branches-on/offstatus

    private final Lock tlock = new ReentrantLock();
    private final Condition transferCondition = tlock.newCondition();
    private final Lock mLock = new ReentrantLock();
    private final Condition markerCondition = mLock.newCondition();


    @Override
    public void initBranch(int balance, List<BranchID> all_branches) throws TException {

        branchBalance = branchBalance + balance;

        for (int i=0; i< all_branches.size(); i++){
            if (all_branches.get(i).getName().equals(name)){
                branchId.setIp(all_branches.get(i).getIp());
                branchId.setPort(all_branches.get(i).getPort());
                branchId.setName(name);
            }
            else
                branchList.add(all_branches.get(i));
        }
        for (int i=0; i< branchList.size(); i++){
            msgReceiveMap.put(branchList.get(i).getName(), 0);
            msgSentMap.put(branchList.get(i).getName(),0);
            }
            //System.out.println(branchList);
            Collections.sort(branchList);
    }


    @Override
    public void transferMoney(TransferMessage message, int messageId) throws  TException {

        tlock.lock();
        try {
            while (msgReceiveMap.get(message.getOrig_branchId().getName()) + 1 != messageId){
                transferCondition.await();
            }
                for (Map.Entry<Integer,SnapshotStatus> entry : snapshotHashMap.entrySet()){
                SnapshotStatus status = entry.getValue();
                if (status.channelStatus.get(message.getOrig_branchId().getName()).equals("on")){
                    /*List temp = status.lSnapshot.getMessages();
                    temp.add(message.getAmount());
                    status.lSnapshot.setMessages(temp);*/
                    int tempChannelBalance = status.channelBalance.get(message.getOrig_branchId().getName());
                    tempChannelBalance = tempChannelBalance + message.getAmount();
                    status.channelBalance.put(message.getOrig_branchId().getName(),tempChannelBalance);
                    System.out.println("balance added "+status.lSnapshot);
                }
                snapshotHashMap.put(entry.getKey(),status);
            }
                branchBalance = branchBalance + message.getAmount();
            int updatedId = msgReceiveMap.get(message.getOrig_branchId().getName()) + 1;
            msgReceiveMap.put(message.getOrig_branchId().getName(), updatedId);

                System.out.println("msgid-" + messageId + " " + branchId.getName() + " got money " + message.getAmount() + " from "
                        + message.getOrig_branchId().getName() + ". Total money " + branchBalance);
           // }
           // tlock.unlock();
            transferCondition.signalAll();
        /*}catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }*/
        }
        catch (InterruptedException e) {
            System.err.println("Interrupted Exception on TransferMoney variable !");
        }

        finally {
            tlock.unlock();
        }
        /*else
        {
            try {
                Random Rsleep = new Random();
                Thread.sleep(Rsleep.nextInt((100-50)+1)+50);
                //Thread.sleep(100);
                transferMoney(message,messageId);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
        }*/
    }

    @Override
    public void Marker(BranchID branchId, int snapshotId, int messageId) throws TException {

        mLock.lock();
        try {
            while (msgReceiveMap.get(branchId.getName()) + 1 != messageId) {
                //while (msgIDno.get(branchId.getName())+1!=messageId){
                markerCondition.await();
            }

            if (markerStatus.containsKey(snapshotId)) {

                if (markerStatus.get(snapshotId).equals(0)) {
                    //marker status snapshot, new-0, already came-1
                    //System.out.println("marker already present loop");
                    //inChannel.put(branchId.getName(), "off");
                    SnapshotStatus status = snapshotHashMap.get(snapshotId);
                    status.channelStatus.put(branchId.getName(), "off");
                    snapshotHashMap.put(snapshotId, status);
                    //markerStatus.put(snapshotId, 1);
                }
            } else {
                //System.out.println("marker firsttime loop");
                snapidlist.add(snapshotId);
                markerStatus.put(snapshotId, 0);

                SnapshotStatus status = new SnapshotStatus();
                status.lSnapshot.setBalance(branchBalance);
                status.lSnapshot.setSnapshotId(snapshotId);
                status.snapshotID = snapshotId;
                snapshotHashMap.put(snapshotId, status);

                //System.out.println("first time marker " + snapshotHashMap.get(snapshotId));

                for (int i = 0; i < branchList.size(); i++) {

                    if (branchList.get(i).getName().equals(branchId.getName())) {
                        status.channelStatus.put(branchList.get(i).getName(), "off");
                    } else {
                        status.channelStatus.put(branchList.get(i).getName(), "on");
                    }
                    status.channelBalance.put((branchList.get(i).getName()),0);
                }
            }
            int updatedId = msgReceiveMap.get(branchId.getName()) + 1;
            msgReceiveMap.put(branchId.getName(), updatedId);
            markerCondition.signalAll();
        }
        catch (InterruptedException e){
            System.err.println("Interrupted Exception on Marker variable!");
        }
    finally {
        mLock.unlock();
            }
        /*}
        else{
            try {
                Random Rsleep = new Random();
                Thread.sleep(Rsleep.nextInt((100-50)+1)+50);
                //Thread.sleep(100);
                Marker(branchId,snapshotId,messageId);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
        }*/
    }

    @Override
    public void initSnapshot(int snapshotId) throws TException {

        final int SSID=snapshotId;
        snapidlist.add(snapshotId);
        SnapshotStatus status = new SnapshotStatus();
        status.lSnapshot.setBalance(branchBalance);
        status.lSnapshot.setSnapshotId(snapshotId);
        status.snapshotID =snapshotId;

        markerStatus.put(snapshotId,0);
        snapshotHashMap.put(snapshotId,status);

        //System.out.println("init snapshot");
        for (int i=0; i<branchList.size();i++){
            status.channelStatus.put(branchList.get(i).getName(),"on");
            status.channelBalance.put((branchList.get(i).getName()),0);

        }
/*        //Runnable markerSendThread = new Runnable() {
          //  @Override
            //public void run() {
                for (int i=0; i<branchList.size();i++){
                    TTransport transport = new TSocket(branchList.get(i).getIp(),
                            branchList.get(i).getPort());
                    try {
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        Branch.Client markerToClient = new Branch.Client(protocol);
                        int messageID = msgSentMap.get(branchList.get(i).getName())+1;
                        msgSentMap.put(branchList.get(i).getName(),messageID);
                        //int messageID = msgIDno.get(branchList.get(i).getName())+1;
                        //msgIDno.put(branchList.get(i).getName(),messageID);
                        markerToClient.Marker(branchId,SSID,messageID);
                        System.out.println("msgid "+messageID+" marker snapid "+SSID+" sending to "+branchList.get(i).getName());


                    }catch (Exception e){
                        System.err.println("marker sending connection failed to"+branchList.get(i).getName());
                        //e.printStackTrace();
                        //System.exit(1);
                    }
                    transport.close();
                    //System.out.println("marker transport layer closed");

                }*/

         //   }
       // };
       // new Thread(markerSendThread).start();
    }

    @Override
    public LocalSnapshot retrieveSnapshot(int snapshotId) throws TException {
        SnapshotStatus status = snapshotHashMap.get(snapshotId);
        List<Integer> templist = status.lSnapshot.getMessages();
        /*for (int i =0; i<status.channelBalance.size();i++){
            //status.lSnapshot.addToMessages(status.channelBalance.get(i));
            templist.add(status.channelBalance.get(i));
        }*/

        for (Map.Entry<String,Integer> entry : status.channelBalance.entrySet()){
            int tempval = entry.getValue();
            templist.add(tempval);
        }
        status.lSnapshot.setMessages(templist);
       System.out.println("Retreive for snapid "+snapshotId+" "+snapshotHashMap.get(snapshotId).lSnapshot);
        //SnapshotStatus status =  snapshotHashMap.get(snapshotId);
        return status.lSnapshot;
    }
}
