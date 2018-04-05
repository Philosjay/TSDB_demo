package testUnits;

import ClientHelpers.CpuInfoCollector;
import ClientHelpers.InfoCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.dao.DBServiceGrpc;
import io.grpc.dao.InfoRequest;
import io.grpc.dao.SQLRequest;
import io.grpc.dao.TableResponse;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client implements Runnable{

    /** 远程链接准备项 **/
    private final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel channel;
    private final DBServiceGrpc.DBServiceBlockingStub blockingStub;
    private  String userName;

    private static float runtime = 0;

    /** 本地监控数据采集准备项 **/
    public  final List<InfoCollector> infoCollectors = new ArrayList<InfoCollector>();


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
    }




    public  void run(){

        /** 完成登录客户端 **/
  //      Client client = new Client("localhost", 50051);
        try {


            /** 初始化，加载监控信息采集器 **/
            mountInfoCollectors(new CpuInfoCollector());
            //           client.mountInfoCollectors(new DiskInfoCollector());
            /** 初始化，更新table及其column**/
            syncRemoteDB();




            List<HashMap<String,Object>> infoList = new ArrayList<>();
            for(int i = 0; i < infoCollectors.size(); i++){
                InfoCollector collector =  infoCollectors.get(i);
                int mapListSize = collector.getInfoHashList().size();

                for (int j = 0; j < mapListSize; j ++){
                    HashMap<String,Object> map = collector.filterInfo(collector.getInfoHashList().get(j));

                    infoList.add(map);
                }
            }

            long startTime=System.currentTimeMillis();//记录开始时间
            while (true){
                if (recordForServerTest(infoList)){
                    break;
                }

            }

            long endTime=System.currentTimeMillis();//记录结束时间
            float excTime=(float)(endTime-startTime)/1000;
            runtime += excTime;
            System.out.println(userName + " done: " + excTime);
            System.out.println("runtime: " + runtime);


        } finally {
            try {
                shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }




    }


    private void putMapIntoRequest(HashMap<String,Object> map, InfoRequest.Builder builder ){
        //遍历HashMap，获得列名称
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            builder.putColumnInfo(key.toString(),value.toString());
        }

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




        for(int i=0; i<infoCollectors.size();i++){
            for (int j=0;j<infoCollectors.get(i).getInfoHashList().size();j++){
                InfoRequest.Builder builder = InfoRequest.newBuilder();

                HashMap<String,Object> map = infoCollectors.get(i).filterInfo(infoCollectors.get(i).getInfoHashList().get(j)) ;
                //更新table 的列

                putMapIntoRequest(map,builder);

                builder.setUserName(userName);
                InfoRequest request = builder.build();
                TableResponse response = blockingStub.updateTables(request);
                logger.info(response.getMesg());
            }
        }



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
    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */



}