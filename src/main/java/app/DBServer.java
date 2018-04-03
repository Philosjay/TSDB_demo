package app;

import ServerHelper.DaoManager;
import ServerHelper.TableNameModifier;
import dao.InfoDao;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.dao.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class  DBServer {
    private static final Logger logger = Logger.getLogger(DBServer.class.getName());

    private Server server;
    private static InfoDao dao = new InfoDao();

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


            InfoDao tmp = new InfoDao();
            tmp.prepareConnection();
            // 一个数据对象对应一张table，如果表不存在添加新table
            if(!tmp.isTableExist(tableName))	tmp.createTable(tableName);

            //更新table 的列
            //遍历HashMap，获得列名称
            Iterator iter = info.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                Object key = entry.getKey();

                if(!tmp.isColExist(key.toString(), tableName)){
                    tmp.addColumn(key.toString(),tableName);
                }

            }

            TableResponse reply = TableResponse.newBuilder().setMesg("update table: " + tableName + " success").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            logger.info("updated table: " + tableName);
            logger.info(req.getColumnInfoMap().toString());
        }


        private int infoCount =0;

        @Override
        public void recordInfo(InfoRequest req, StreamObserver<TableResponse> responseObserver){
            /** 录入信息时, 调出该table 专属的Dao **/
            String tableName = TableNameModifier.generateTableName(req.getColumnInfoMap().get("name"),req.getUserName());



            HashMap<String,Object> info = new HashMap<String, Object>(req.getColumnInfoMap());
            if(infoCount == 0){
                dao.prepareConnection();
                dao.prepareBatch(info,tableName);
            }

            dao.addInfoToBatch(info,tableName);
            logger.info("recorded info for " + infoCount  + "th");

            if(infoCount == 1000){
                dao.executeBatch();
                TableResponse reply = TableResponse.newBuilder().setIsExist(true).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }else {
                TableResponse reply = TableResponse.newBuilder().setIsExist(false).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }




            logger.info(req.getColumnInfoMap().toString());
            infoCount ++;
        }

        @Override
        public void findInfo(InfoRequest req, StreamObserver<TableResponse> responseObserver){
            logger.info("tring to find info for: " + req.getDevName());

            String tableName = TableNameModifier.generateTableName(req.getDevName(),req.getUserName());

            HashMap<String ,Object> infoMap = new HashMap<>(req.getColumnInfoMap());
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
