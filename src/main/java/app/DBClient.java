package app;

import ClientHelpers.CpuInfoCollector;
import ClientHelpers.DiskInfoCollector;
import ClientHelpers.InfoCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.dao.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBClient {

    /** 远程链接准备项 **/
    private static final Logger logger = Logger.getLogger(DBClient.class.getName());
    private final ManagedChannel channel;
    private final DBServiceGrpc.DBServiceBlockingStub blockingStub;
    private  String userName;

    private void setUserName(String userName){
        this.userName = userName;
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


    /** 本地监控数据采集准备项 **/
    public  static final List<InfoCollector> infoCollectors = new ArrayList<InfoCollector>();



    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public DBClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /** Construct client for accessing RouteGuide server using the existing channel. */
    DBClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = DBServiceGrpc.newBlockingStub(channel);
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
        logger.info("Will try to sync  " + " ...");




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


    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {

        /** 完成登录客户端 **/
        DBClient client = new DBClient("localhost", 50051);
        try {

            /* Access a service running on the local machine on port 50051 */
            String user = "Link";
            if (args.length > 0) {
                user = args[0]; /* Use the arg as the name to greet if provided */
            }


            if(!client.signIn(user)){
                client.signUp(user);
            }

            client.setUserName(user);


            /** 初始化，加载监控信息采集器 **/
            client.mountInfoCollectors(new CpuInfoCollector());
            client.mountInfoCollectors(new DiskInfoCollector());
            /** 初始化，更新table及其column**/
            client.syncRemoteDB();




            /** 开始录入监控信息 **/
            client.recordInfo();

            String devName = "cpu1";
            String[] infoType = {"time", "userUseRate", "totalUseRate"};
            List<HashMap<String,String>> mapList =  client.findInfo(devName, infoType);
            for (int i=0; i< mapList.size(); i++){
                logger.info(mapList.get(i).toString());
            }


            String sql = "select * from tb_cpu3_Harry ";
            client.executeSQLForQuery(sql);

            sql = "delete from tb_cpu5_Harry ";
            client.executeSQLForUpdate(sql);




        } finally {
            client.shutdown();
        }




    }


}
