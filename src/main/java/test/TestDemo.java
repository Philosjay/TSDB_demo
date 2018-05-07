package test;


import ServerHelper.ArgsForTableAndBatch;
import ServerHelper.InfoHolder;
import ServerHelper.InfoManager;
import ServerHelper.InfoReceiver;
import ServerHelper.daoHelper.DaoManager;
import ServerHelper.daoHelper.DaoManagerDistributer;
import ServerHelper.daoHelper.ThreadForBatchInsert;
import clientHelpers.CpuInfoCollector;
import dao.InfoDao;
import org.junit.Test;
import utils.JdbcUtils;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestDemo {

    @Test
    public void testReceiver(){
        InfoReceiver receiver = new InfoReceiver();

        HashMap<String,String> info = new HashMap<>();


        long start = System.currentTimeMillis();
        for (int i=0;i<10000000;i++){
            receiver.recForTest(info);
        }
        long end = System.currentTimeMillis();

        System.out.println((float)(end-start)/1000);

    }

    @Test
    public void distributerTest(){
        try {

            int TOTALINSERTS = 1200000;
            int INSERTSPERBATCH = 10000;

            String tableName = "tb_batch_test";

            CpuInfoCollector collector = new CpuInfoCollector();
            Map<String,Object> map = collector.getInfoMapByIndex(0);

            map.remove("userUseRate");
            map.remove("sysUseRate");
            map.remove("waitRate");
            map.remove("errorRate");
            map.remove("idleRate");

            map.remove("time");
            map.remove("type");
            map.remove("name");

            DaoManagerDistributer distributer = new DaoManagerDistributer();
            distributer.prepareDao(tableName,map);


            InfoDao dao = distributer.getDaoManager().getDao();
            if(!dao.isTableExist(tableName))	dao.createTable(tableName);

            Iterator iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                Object key = entry.getKey();

                if(!dao.isColExist(key.toString(), tableName)){
                    dao.addColumn(key.toString(),tableName);
                }

            }

            InfoHolder[] infos = new InfoHolder[INSERTSPERBATCH];

            for (int i=0;i<INSERTSPERBATCH;i++){
                infos[i] = new InfoHolder(map);
            }

            Thread[] thread = new Thread[TOTALINSERTS/INSERTSPERBATCH];

            long start = System.currentTimeMillis();
            for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){
                thread[i] = new Thread(() ->
                        distributer.getDaoManager().addInfoANDRequireBatchExcecution(infos));
                thread[i].start();
            }



            long end = System.currentTimeMillis();
            System.out.println((float)(end-start)/1000);
            start = System.currentTimeMillis();

            for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){
                thread[i].join();
            }

            end = System.currentTimeMillis();
            System.out.println((float)(end-start)/1000);




        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void batchTest_one_Thread(){
        // 测试JDBC 批处理的性能
        try {
            int TOTALINSERTS = 1200000;
            int INSERTSPERBATCH = 10000;

            String tableName = "tb_batch_test";

            CpuInfoCollector collector = new CpuInfoCollector();
            Map<String,Object> map = collector.getInfoMapByIndex(0);

            map.remove("userUseRate");
            map.remove("sysUseRate");
            map.remove("waitRate");
            map.remove("errorRate");
            map.remove("idleRate");


            map.remove("time");
            map.remove("type");
            map.remove("name");

            DaoManager mng = new DaoManager();
            mng.prepareDao(tableName,map);

            InfoDao dao = mng.getDao();
            if(!dao.isTableExist(tableName))	dao.createTable(tableName);

            Iterator iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                Object key = entry.getKey();

                if(!dao.isColExist(key.toString(), tableName)){
                    dao.addColumn(key.toString(),tableName);
                }

            }

            InfoHolder[] infos = new InfoHolder[INSERTSPERBATCH];

            for (int i=0;i<INSERTSPERBATCH;i++){
                infos[i] = new InfoHolder(map);
            }



            long start = System.currentTimeMillis();
            for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){
                mng.addInfoANDRequireBatchExcecution(infos);
                System.out.println((i));
            }


            long end = System.currentTimeMillis();
            System.out.println((float)(end-start)/1000);



            Thread.sleep(5000);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void batchTest_muti_Thread(){
        // 测试JDBC 批处理的性能
        try {
            int TOTALINSERTS = 200000;
            int INSERTSPERBATCH = 10000;

            String tableName = "tb_batch_test";

            CpuInfoCollector collector = new CpuInfoCollector();
            Map<String,Object> map = collector.getInfoMapByIndex(0);

            DaoManager mng = new DaoManager();
            mng.prepareDao(tableName,map);

            InfoDao dao = mng.getDao();
            if(!dao.isTableExist(tableName))	dao.createTable(tableName);

            Iterator iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                Object key = entry.getKey();

                if(!dao.isColExist(key.toString(), tableName)){
                    dao.addColumn(key.toString(),tableName);
                }

            }

            InfoHolder[] infos = new InfoHolder[INSERTSPERBATCH];

            for (int i=0;i<INSERTSPERBATCH;i++){
                infos[i] = new InfoHolder(map);
            }


            Thread[] thread = new Thread[TOTALINSERTS/INSERTSPERBATCH];

            long start = System.currentTimeMillis();
            for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){
                thread[i] = new Thread(new ThreadForBatchInsert(mng,infos,0));
                thread[i].start();
                //           mng.addInfoANDRequireBatchExcecution(infos);
                System.out.println((i));
            }

            for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){
                thread[i].join();
            }


            long end = System.currentTimeMillis();
            System.out.println((float)(end-start)/1000);




        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void multi_table_test(){
        try {

            int TABLECOUNT = 10;
            int TOTALINSERTS = 500000;
            int INSERTSPERBATCH = 10000;
            int CYCLE = 8;

            String[] tableNames = new String[TABLECOUNT];

            CpuInfoCollector collector = new CpuInfoCollector();
            Map<String,Object> map = collector.getInfoMapByIndex(0);

            map.remove("userUseRate");
            map.remove("sysUseRate");
            map.remove("waitRate");
            map.remove("errorRate");
            map.remove("idleRate");

            map.remove("time");
            map.remove("type");
            map.remove("name");

            DaoManagerDistributer[] distributers = new DaoManagerDistributer[TABLECOUNT];

            for (int i=0; i<TABLECOUNT; i++){
                tableNames[i] = "tb_test" + i;
                distributers[i] = new DaoManagerDistributer();
                distributers[i].prepareDao(tableNames[i],map);

                InfoDao dao = distributers[i].getDaoManager().getDao();
                if (!dao.isTableExist(tableNames[i])){
                    dao.createTable(tableNames[i]);
                }
            }


            Iterator iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                Object key = entry.getKey();

                for (int i=0; i<TABLECOUNT; i++){
                    InfoDao dao = distributers[i].getDaoManager().getDao();
                    if(!dao.isColExist(key.toString(), tableNames[i])){
                        dao.addColumn(key.toString(),tableNames[i]);
                    }
                }

            }


            InfoHolder[] infos = new InfoHolder[INSERTSPERBATCH];

            for (int i=0;i<INSERTSPERBATCH;i++){
                infos[i] = new InfoHolder(map);
            }



            for (int j=0;j<CYCLE;j++){


                Thread[] thread = new Thread[TOTALINSERTS/INSERTSPERBATCH];

                long start = System.currentTimeMillis();
                for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++){

                    int index = i%TABLECOUNT;

   //                 thread[i] = new Thread(() ->
    //                        distributers[index].getDaoManager().addInfoANDRequireBatchExcecution(infos));
                    thread[i] = new Thread(new ThreadForBatchInsert(distributers[index].getDaoManager(),infos,0));


                    thread[i].start();
                }



                long end = System.currentTimeMillis();
                System.out.println((float)(end-start)/1000);
                start = System.currentTimeMillis();

                for (int i=0;i<TOTALINSERTS/INSERTSPERBATCH;i++) {
                    thread[i].join();
                }
                end = System.currentTimeMillis();
                System.out.println("cycle " + j + " : " + (float)(end-start)/1000);
            }






        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void InfoManager_Test_packetInsert(){
        InfoDuplicator dpl = new InfoDuplicator();

        InfoHolder[] info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);


        InfoManager infoMng = new InfoManager();
        infoMng.init(info[0].map);


        long start = System.currentTimeMillis();

        for (int j=0;j<100;j++){

            for (int i=0;i<200000/ArgsForTableAndBatch.activeBatch;i++){
                infoMng.receiveInfo(info);

                info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);
            }



            int freeThread = infoMng.getFreeThreadCount();
            System.out.println("free thread " + freeThread);


            long end = System.currentTimeMillis();

            long insertTime = ( end - start);
            if (insertTime < 1000){
                try {

                    Thread.sleep(1000 - insertTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            end = System.currentTimeMillis();
            System.out.println("_____________________________________ " + (float)(end-start)/1000);
            start = System.currentTimeMillis();
        }


        infoMng.complete();


        long end = System.currentTimeMillis();
        System.out.println((float)(end-start)/1000);


        count_row();

    }


    @Test
    public void InfoManager_Test_insertVSbatch(){

        InfoDuplicator dpl = new InfoDuplicator();

        InfoHolder[] info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);


        InfoManager infoMng = new InfoManager();
        infoMng.init(info[0].map);



        long start = System.currentTimeMillis();


        for (int j=0;j<4;j++){


            for (int i=0;i<200000/ArgsForTableAndBatch.activeBatch;i++){
                infoMng.receiveInfo(info);

                info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);
            }

            long end = System.currentTimeMillis();
            float insertTime = (float)(end-start)/1000;

            System.out.println("insert " + insertTime);
            start = System.currentTimeMillis();


            infoMng.complete();


            System.out.println(infoMng.getFreeThreadCount());

            end = System.currentTimeMillis();
            System.out.println("batch " + (float)(end-start)/1000);
            start = System.currentTimeMillis();
        }




        count_row();

    }

    @Test
    public void count_row(){
        CpuInfoCollector collector = new CpuInfoCollector();
        Map<String,Object> map = collector.getInfoMapByIndex(0);

        map.remove("userUseRate");
        map.remove("sysUseRate");
        map.remove("waitRate");
        map.remove("errorRate");
        map.remove("idleRate");

        map.remove("time");
        map.remove("name");

        InfoManager infoMng = new InfoManager();
        infoMng.init(map);


        long count =0;
        for (int i=0;i<ArgsForTableAndBatch.activeTable;i++){
            count += infoMng.getDao().getRowCountInTable("tb_test" + i);
        }

        System.out.println("count : " + count);
    }

    @Test
    public void osUtil_test(){
        CpuInfoCollector collector = new CpuInfoCollector();


        int count =0;
        while (true){
            List<HashMap<String,Object>> map = collector.getInfoHashList();
            for (Map<String,Object> m:
                 map) {
                m.remove("userUseRate");
                m.remove("sysUseRate");
                m.remove("waitRate");
                m.remove("errorRate");
                m.remove("idleRate");

            }
            System.out.println(count++);
        }
    }

    @Test
    public void InfoDuplicator_test(){

        InfoDuplicator dpl = new InfoDuplicator();

        InfoHolder[] infos = dpl.getInfos(ArgsForTableAndBatch.activeBatch);

        System.out.println(infos.length);

//        for (int i=0;i<infos.length;i++){
//            Iterator iter = infos[i].map.entrySet().iterator();
//            while (iter.hasNext()) {
//                Map.Entry entry = (Map.Entry)iter.next();
//                Object key = entry.getKey();
//                Object value = entry.getValue();
//
//    //            System.out.println(key + "---" + value);
//            }
//        }

        System.out.println(infos.length);

    }

//    @Test
    static public void main(String[] args){
        ShellForTSDBControl sh = new ShellForTSDBControl();
        sh.run();
    }

}
