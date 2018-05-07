package clientTest;

import clientHelpers.CpuInfoCollector;
import clientHelpers.InfoCollector;
import clientHelpers.ResponseStreamObserverImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.dao.*;
import io.grpc.stub.StreamObserver;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client implements Runnable{
    private final int MAXINFO = 600000 ;
    private final int INFOPERCOMMIT = 100000;
    private final int PACKETSIZE = 10000;


    /** 远程链接准备项 **/
    private final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel channel;
    private final DBServiceGrpc.DBServiceBlockingStub blockingStub;
    private  String userName;
    private final DBServiceGrpc.DBServiceStub asyncStub;//非阻塞,异步存根


    /** 本地监控数据采集准备项 **/
    public   final List<InfoCollector> infoCollectors = new ArrayList<InfoCollector>();


    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public Client(String host, int port, String clientName) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());

        userName = clientName;
    }

    /** Construct client for accessing RouteGuide server using the existing channel. */
    Client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = DBServiceGrpc.newBlockingStub(channel);
        asyncStub = DBServiceGrpc.newStub(channel);
    }



    public void run(){

        //      Client client = new Client("localhost", 50051,"Client" + 1);
        /** 完成登录客户端 **/
        try {

            /** 初始化，加载监控信息采集器 **/
            mountInfoCollectors(new CpuInfoCollector());
            //           client.mountInfoCollectors(new DiskInfoCollector());
            /** 初始化，更新table及其column**/
            syncRemoteDB();


            try {

                InfoRequest request = getRequestPacket();

                    recordInfoByStream_chat_packet(request);





            } catch (Exception e) {
                e.printStackTrace();
            }




        } finally {
            try {
                shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void putMapIntoRequest(Map map, InfoRequest.Builder builder ){
        //遍历HashMap，获得列名称
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            builder.putColumnInfo(key.toString(),value.toString());
        }

    }
    private void packIntoBuilder(Map map, InfoRequest.Builder builder ){

        InfoMap.newBuilder();
        HashMap<String,String> infoMap = new HashMap<>();
        //遍历HashMap，获得列名称
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            infoMap.put(key.toString(),value.toString());

        }
        builder.addInfoPacket(InfoMap.newBuilder().putAllInfoMap(infoMap));

    }



    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /** 尝试登录 **/
    public boolean signIn(String name) {
        logger.info("Will try to sign in  " + name + " ...");

        InfoRequest.Builder builder = InfoRequest.newBuilder();
        InfoRequest request = builder.build();
        TableResponse response = null;

        try {
            response = blockingStub.isTableExist(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }

        logger.info("Result: " + response.getIsExist());
        return response.getIsExist();
    }

    /** 尝试注册 **/
    public boolean signUp(String clientName){
        logger.info("Will try to singUp  " + " ...");

        InfoRequest.Builder builder = InfoRequest.newBuilder();
        builder.putColumnInfo("name","string");

        InfoRequest request = builder.build();

        return false;
    }


    /** 根据client 现有的设备及其监控条目，更新远端数据库的table 及其column  **/
    public void mountInfoCollectors(InfoCollector collector){
        infoCollectors.add(collector);


    }

    public void syncRemoteDB(){
        logger.info(userName + " will try to sync  " + " ...");


        // cpu0 信息
        Map<String,Object> map = infoCollectors.get(0).getInfoMapByIndex(0);
        map.remove("userUseRate");
        map.remove("sysUseRate");
        map.remove("waitRate");
        map.remove("errorRate");
        map.remove("idleRate");

        map.remove("time");
        map.remove("type");
        map.remove("name");


        InfoRequest.Builder builder = InfoRequest.newBuilder();

        //更新table 的列

        putMapIntoRequest(map,builder);

        builder.setUserName(userName);
        InfoRequest request = builder.build();
        TableResponse response = blockingStub.updateTables(request);
        logger.info(response.getMesg());



        return ;
    }

    public void recordInfo(){
        for(int i = 0; i < infoCollectors.size(); i++){
            InfoCollector collector =  infoCollectors.get(i);
            int mapListSize = collector.getInfoHashList().size();

            for (int j = 0; j < mapListSize; j ++){
                HashMap<String,Object> map = collector.filterInfo(collector.getInfoHashList().get(j));

                InfoRequest.Builder builder = InfoRequest.newBuilder();
                //更新table 的列

                putMapIntoRequest(map,builder);
                builder.setUserName(userName);
                InfoRequest request = builder.build();
                TableResponse response = blockingStub.recordInfo(request);

                logger.info(response.getMesg());
            }
        }


    }



    public List<HashMap<String,String>> findInfo(String devName, String[] infoType){
        logger.info("=========================================================");
        logger.info("finding info for: " + devName);

        InfoRequest.Builder builder = InfoRequest.newBuilder();
        builder.setDevName(devName);
        builder.setUserName(userName);
        for (String type:
                infoType) {
            builder.putColumnInfo(type,"");
        }

        InfoRequest request = builder.build();
        TableResponse response = blockingStub.findInfo(request);


        List<HashMap<String,String>> mapList = getMapListFromResponse(response);


        logger.info(response.getMesg());
        return mapList;
    }


    public void executeSQLForQuery(String sql){
        logger.info("=========================================================");
        logger.info("excecuting sql for Query...");

        SQLRequest.Builder builder = SQLRequest.newBuilder();
        builder.setSql(sql);
        SQLRequest request = builder.build();
        TableResponse response = blockingStub.executeSQLForQuery(request);

        List<HashMap<String,String>> mapList = getMapListFromResponse(response);
        for (int i=0;i<mapList.size();i++){
            System.out.println(mapList.get(i));
        }
    }

    public void executeSQLForUpdate(String sql){
        logger.info("=========================================================");
        logger.info("excecuting sql for update...");

        SQLRequest.Builder builder = SQLRequest.newBuilder();
        builder.setSql(sql);
        SQLRequest request = builder.build();
        TableResponse response = blockingStub.executeSQLForUpdate(request);

        logger.info(response.getMesg());
    }

    private List<HashMap<String,String>> getMapListFromResponse(TableResponse response){
        List<HashMap<String,String>> mapList = new ArrayList<HashMap<String,String>>();
        for (int i=0; i< response.getResultMapListList().size(); i++){
            mapList.add(new HashMap<String, String>(response.getResultMapListList().get(i).getInfoMapMap()));
        }
        return mapList;
    }



    public boolean recordForServerTest(List<HashMap<String,Object>> infoList){

        boolean isEnd = false;

        for (int i=0;i<infoList.size();i++){
            HashMap<String,Object> map = infoList.get(i);

            InfoRequest.Builder builder = InfoRequest.newBuilder();
            //更新table 的列

            putMapIntoRequest(map,builder);
            builder.setUserName(userName);
            InfoRequest request = builder.build();
            TableResponse response = blockingStub.recordInfo(request);

            logger.info(response.getMesg());


            if (response.getIsExist()){
                isEnd = true;
                break;
            }
        }

        return isEnd;
    }

    private List<InfoRequest> getRequestList(){

        List<HashMap<String,Object>> infoList = new ArrayList<HashMap<String,Object>>();

        for(int i = 0; i < infoCollectors.size(); i++){
            InfoCollector collector =  infoCollectors.get(i);
            int mapListSize = collector.getInfoHashList().size();

            for (int j = 0; j < mapListSize; j ++){
                HashMap<String,Object> map = collector.filterInfo(collector.getInfoHashList().get(j));
                infoList.add(map);
            }
        }

        List<InfoRequest> reqList = new ArrayList<InfoRequest>();

        int count=0;
        while (true){

            for (HashMap<String,Object> map:
                    infoList) {

                if (count == INFOPERCOMMIT){
                    break;
                }


                InfoRequest.Builder builder = InfoRequest.newBuilder();
                //更新table 的列


                builder.setUserName(userName);
                map.replace("userUseRate",count + 1);
                putMapIntoRequest(map,builder);
                builder.setMesg(count + "");

//                if (count == MAXINFO - 1){
//                    // 消息结尾
//                    builder.setIsFinal(true);
//                }

                InfoRequest request = builder.build();
                reqList.add(request);

                count++;
            }
            if (count == INFOPERCOMMIT){
                break;
            }
        }
        return reqList;
    }

    private InfoRequest getRequestPacket(){

        // cpu0 信息
        Map<String,Object> map = infoCollectors.get(0).getInfoMapByIndex(0);
        map.remove("userUseRate");
        map.remove("sysUseRate");
        map.remove("waitRate");
        map.remove("errorRate");
        map.remove("idleRate");

        map.remove("time");
        map.remove("type");
        map.remove("name");




        InfoRequest.Builder builder = InfoRequest.newBuilder();

        int count=0;
        while (count<PACKETSIZE){

        builder.setUserName(userName);
        map.replace("userUseRate",count + 1);
        packIntoBuilder(map,builder);
        builder.setDevName("cpu");
        builder.setMesg(count + "");

        count++;

        }
        return builder.build();
    }

    private List<List<InfoRequest>> getRequestPacketList(){

        List<HashMap<String,Object>> infoList = new ArrayList<>();
        List<List<InfoRequest>> reqPacketList = new ArrayList<>();
        for(int i = 0; i < infoCollectors.size(); i++){
            InfoCollector collector =  infoCollectors.get(i);
            int mapListSize = collector.getInfoHashList().size();

            for (int j = 0; j < mapListSize; j ++){
                HashMap<String,Object> map = collector.filterInfo(collector.getInfoHashList().get(j));
                infoList.add(map);
            }
        }

        List<InfoRequest> reqList = new ArrayList<>();

        int count=0;
        while (true){

            for (HashMap<String,Object> map:
                    infoList) {

                if (count == MAXINFO){
                    break;
                }

                if (count%400000==0 && count>0){
                    //每200000 分为一个packet
                    reqPacketList.add(reqList);
                    reqList = new ArrayList<InfoRequest>();
                }

                InfoRequest.Builder builder = InfoRequest.newBuilder();
                //更新table 的列


                builder.setUserName(userName);
                map.replace("userUseRate",count + 1);
                putMapIntoRequest(map,builder);
                builder.setMesg(count + "");

                if (count == MAXINFO - 1){
                    // 消息结尾
                    builder.setIsFinal(true);
                    InfoRequest request = builder.build();
                    reqList.add(request);
                    reqPacketList.add(reqList);
                }

                InfoRequest request = builder.build();
                reqList.add(request);

                count++;
            }
            if (count == MAXINFO){
                break;
            }
        }
        return reqPacketList;
    }

    public void recordInfoByBlocked(InfoRequest infoPacket){


                InfoRequest.Builder builder = InfoRequest.newBuilder();
                //更新table 的列

                builder.setUserName(userName);
                InfoRequest request = builder.build();
                TableResponse response = blockingStub.recordInfo(infoPacket);

                logger.info(response.getMesg());
    }


    public void recordInfoByStream(List<InfoRequest> infoList) throws InterruptedException {
        logger.info("*** RecordRoute");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<TableResponse> responseObserver = new StreamObserver<TableResponse>() {

            @Override
            public void onNext(TableResponse response) {

            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                logger.info("RecordRoute 1 Failed: {0}" + status.toString());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished RecordRoute 1");
                finishLatch.countDown();
            }
        };

        StreamObserver<InfoRequest> requestObserver = asyncStub.recordInfoByStream(responseObserver);

        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < infoList.size(); ++i) {

                requestObserver.onNext(infoList.get(i));

                if (i%(200000) == 200000-1 ){
                    long end = System.currentTimeMillis();
                    System.out.println(i +"th          " + (float)(end-start)/1000);

                    start = System.currentTimeMillis();
                }

                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    System.out.println("countDown is ZERO");
                    return;
                }
            }

        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // Receiving happens asynchronously
        finishLatch.await(1, TimeUnit.MINUTES);
    }

    int count = 0;
    public void recordInfoByStream_chat(List<InfoRequest> infoList){
//        logger.info("*** RecordRoute");
        final CountDownLatch finishLatch = new CountDownLatch(1);


        ResponseStreamObserverImpl responseStreamObserver = new ResponseStreamObserverImpl(finishLatch);
        //写入监听
        StreamObserver<InfoRequest> requestObserver = asyncStub.recordInfoByStreamChat(responseStreamObserver);
                //写回监听

        try {
            for (int i=0;i<infoList.size();i++){

                requestObserver.onNext(infoList.get(i));

                count++;
            }


        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        } //标识写完

        requestObserver.onCompleted();
    }


    public void recordInfoByStream_chat_packet(InfoRequest infoPacket){
        final CountDownLatch finishLatch = new CountDownLatch(1);


        ResponseStreamObserverImpl responseStreamObserver = new ResponseStreamObserverImpl(finishLatch);
        //写入监听
        StreamObserver<InfoRequest> requestObserver = asyncStub.recordInfoByStreamPacketChat(responseStreamObserver);
        //写回监听

        long start = System.currentTimeMillis();
        for (int j=0;j<120;j++){

            try {
                requestObserver.onNext(infoPacket);
                count++;

            } catch (RuntimeException e) {
                requestObserver.onError(e);
                throw e;
            } //标识写完



            if (j%20 == 0){
//                while (!responseStreamObserver.toContinue()){
//
//                }

                try {
                    Thread.sleep(800);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("             " + (float)(end -start)/1000);
                start = System.currentTimeMillis();
            }


        }



        requestObserver.onCompleted();
    }
}