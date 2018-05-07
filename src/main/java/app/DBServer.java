package app;

import ServerHelper.InfoHolder;
import ServerHelper.InfoManager;
import ServerHelper.daoHelper.DaoManager;
import ServerHelper.daoHelper.DaoManagerDistributer;
import ServerHelper.InfoReceiver;
import ServerHelper.TableNameModifier;
import ServerHelper.daoHelper.ThreadForBatchInsert;
import dao.InfoDao;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.dao.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class  DBServer {
    static private final Logger logger = Logger.getLogger(DBServer.class.getName());

    private Server server;




    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new DBServer.DaoImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                DBServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }



    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final DBServer server = new DBServer();
        server.start();
        server.blockUntilShutdown();
    }










    static class DaoImpl extends DBServiceGrpc.DBServiceImplBase {

        private int infoCount=0;
        long startTime;
        private InfoManager infoManager = new InfoManager();


        private void putMapListIntoResponse(List<HashMap<String ,Object>> mapList, TableResponse.Builder builder){

            HashMap<String ,Object> infoMap;
            for (int i=0; i<mapList.size(); i++){
                infoMap = mapList.get(i);
                InfoMap.Builder mapBuilder = InfoMap.newBuilder();

                //遍历HashMap，获得列名称
                Iterator iter = infoMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry)iter.next();
                    Object key = entry.getKey();
                    Object value = entry.getValue();

                    mapBuilder.putInfoMap(key.toString(),value.toString());

                }
                builder.addResultMapList(i, mapBuilder.build());
            }
        }

        @Override
        public void isTableExist(InfoRequest req, StreamObserver<TableResponse> responseObserver) {

            TableResponse reply = TableResponse.newBuilder().setIsExist(true).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void updateTables(InfoRequest req, StreamObserver<TableResponse> responseObserver) {

            HashMap<String,Object> info = new HashMap<String, Object>(req.getColumnInfoMap());

            String tableName = TableNameModifier.generateTableName(req.getColumnInfoMap().get("name"),req.getUserName());

            infoManager.init(info);



            TableResponse reply = TableResponse.newBuilder().setMesg("update table: " + tableName + " success").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            logger.info("updated table: " + tableName);
            logger.info(req.getColumnInfoMap().toString());
        }



        @Override
        public void recordInfo(InfoRequest req, StreamObserver<TableResponse> responseObserver){
            if(infoCount ==1 ){
                startTime=System.currentTimeMillis();//记录开始时间
            }


            String tableName = TableNameModifier.generateTableName(req.getDevName(),req.getUserName());
            List<InfoMap> infoPacket = req.getInfoPacketList();


            TableResponse reply = TableResponse.newBuilder().setMesg("record info for : " + tableName + " success").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        }


        @Override
        public  StreamObserver<InfoRequest> recordInfoByStreamChat(final StreamObserver<TableResponse> responseObserver) {

            return new StreamObserver<InfoRequest>() {

                @Override
                public void onNext(InfoRequest req) {
                    if(infoCount ==1 ){
                        startTime=System.currentTimeMillis();//记录开始时间
                    }


                    String tableName = TableNameModifier.generateTableName(req.getColumnInfoMap().get("type"),req.getUserName());
                    Map<String,String> info = new HashMap<>(req.getColumnInfoMap());



//                    daoManagerDistributerMap.get(tableName).getDaoManager(0).addInfoANDRequireBatchExcecution(info);
//                    int count = daoManagerDistributerMap.get(tableName);


//
//
//                    receiver.receiveInfo(info,daoManagerDistributerMap.get(tableName),tableName);
//                    int count = receiver.infoCount;
//
//                    if (count%200000 ==0){
//                        long end = System.currentTimeMillis();
//                        System.out.println(count + "th     "  + tableName + "    " + (float)(end-startTime)/1000 + "    "+ receiver.infoCount);
//                        startTime = System.currentTimeMillis();
//
//                        TableResponse response = TableResponse.newBuilder().setMesg("recording " + count + "th" ).build();
//
//                        responseObserver.onNext(response);
//                    }


                    infoCount++;
                }


                @Override
                synchronized public void onError(Throwable t) {
                    logger.info("Encountered error in recordInfoByStream");
                }

                @Override
                synchronized public void onCompleted() {
                    responseObserver.onNext(TableResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }

            };

        }

        @Override
        public  StreamObserver<InfoRequest> recordInfoByStreamPacketChat(final StreamObserver<TableResponse> responseObserver) {

            return new StreamObserver<InfoRequest>() {

                @Override
                public void onNext(InfoRequest req) {
                    if(infoCount ==1 ){
                        startTime=System.currentTimeMillis();//记录开始时间
                    }


                    List<InfoMap> infoPacket = req.getInfoPacketList();


                    for (int i=0;i<infoPacket.size();i++){
//                        long count = infoManager.receiveInfo(new InfoHolder(infoPacket.get(i).getInfoMapMap()));
//
//                        if (count%200000 ==0){
//                            long end = System.currentTimeMillis();
//                            System.out.println((float)(end-startTime)/1000);
//                            startTime = System.currentTimeMillis();
//
//                            TableResponse response = TableResponse.newBuilder().setMesg("recording " + count + "th" ).build();
//
//                            responseObserver.onNext(response);
//                        }


                    }




             //       System.out.println(count);



//                    infoCount++;
                }


                @Override
                synchronized public void onError(Throwable t) {
                    logger.info("Encountered error in recordInfoByStream");
                }

                @Override
                synchronized public void onCompleted() {
                    responseObserver.onNext(TableResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }

            };

        }

        @Override
        public  StreamObserver<InfoRequest> recordInfoByStream(final StreamObserver<TableResponse> responseObserver) {

            return new StreamObserver<InfoRequest>() {

                @Override
                public void onNext(InfoRequest req) {
                    if(infoCount ==1 ){
                        startTime=System.currentTimeMillis();//记录开始时间
                    }

                    String tableName = TableNameModifier.generateTableName(req.getColumnInfoMap().get("name"),req.getUserName());
                    HashMap<String,Object> info = new HashMap<String, Object>(req.getColumnInfoMap());

//                    daoManagerDistributerMap.get(tableName).getDaoManager().addInfoANDRequireBatchExcecution(info);
//                    int count = daoManagerDistributerMap.get(tableName).countInsert();
//                    if (count%200000 ==0){
//                        long end = System.currentTimeMillis();
//                        System.out.println(count + "th     "  + tableName + "    " + (float)(end-startTime)/1000);
//                        startTime = System.currentTimeMillis();
//
//                    }
//
//                    infoCount++;
                }

                @Override
                synchronized public void onError(Throwable t) {
                    logger.info("Encountered error in recordInfoByStream");
                }

                @Override
                synchronized public void onCompleted() {
                    responseObserver.onNext(TableResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }


        @Override
        public void findInfo(InfoRequest req, StreamObserver<TableResponse> responseObserver){
            logger.info("tring to find info for: " + req.getDevName());

            String tableName = TableNameModifier.generateTableName(req.getDevName(),req.getUserName());
            InfoDao dao = infoManager.getDao();

            HashMap<String ,Object> infoMap = new HashMap<String ,Object>(req.getColumnInfoMap());
            List<HashMap<String ,Object>> mapList = dao.findInfo(tableName,infoMap);

            for (int i=0; i< mapList.size(); i++){
                logger.info(mapList.toString());
            }



            TableResponse.Builder builder = TableResponse.newBuilder();

            putMapListIntoResponse(mapList,builder);

            TableResponse response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("found info for " + tableName + ": " + mapList.toString());

        }

        @Override
        public void executeSQLForQuery(SQLRequest request, StreamObserver<TableResponse> responseObserver) {
            /** 执行sql时, 调出该table 专属的Dao **/

            logger.info("tring to executeSQL ");
            InfoDao dao = new InfoDao();
            dao.prepareConnection();

            List<HashMap<String ,Object>> mapList = dao.executeQuery(request.getSql());
            dao.closeConnection();


            HashMap<String ,Object> infoMap ;
            TableResponse.Builder builder = TableResponse.newBuilder();
            putMapListIntoResponse(mapList,builder);

            TableResponse reply = builder.setMesg("executeSQL success").build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        }

        @Override
        public void executeSQLForUpdate(SQLRequest request, StreamObserver<TableResponse> responseObserver) {
            /** 执行sql时, 调出该table 专属的Dao **/

            logger.info("tring to executeSQL ");
            InfoDao dao = new InfoDao();
            dao.prepareConnection();

            dao.executeUpdate(request.getSql());
            dao.closeConnection();

            TableResponse.Builder builder = TableResponse.newBuilder();
            TableResponse reply = builder.setMesg("executeSQL success").build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        }



    }
}
