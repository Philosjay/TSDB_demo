package ServerHelper;

import dao.InfoDao;

import java.util.HashMap;

public class DaoManager {
    private HashMap<String, InfoDao> daoHashMap = new HashMap<String, InfoDao>();
    private HashMap<String, BatchExcecutionJudger> batchExcecutionJudgerMap= new HashMap<>();
    long startTime = 0; //程序开始记录时间。。


    public void claimDao(String tableName, InfoDao dao){

        daoHashMap.put(tableName,dao);
        batchExcecutionJudgerMap.put(tableName,new BatchExcecutionJudger());
    }

    public InfoDao getDao(String tableName){
        return daoHashMap.get(tableName);
    }


    public void addInfoToBatch(HashMap<String,Object> info, String tableName){
        if (startTime == 0){
            startTime = System.currentTimeMillis();
        }

        InfoDao dao = getDao(tableName);

        BatchExcecutionJudger batchManager = batchExcecutionJudgerMap.get(tableName);
        dao.addInfoToBatch(info,tableName,batchManager.getCurrentPstmIndex());
        batchManager.countInsert();
    }

    public void requireBatchExcecution(String tableName, boolean isFinal){

        BatchExcecutionJudger batchManager = batchExcecutionJudgerMap.get(tableName);

        if (isFinal || batchManager.toExcBatch()){
            Thread thrd = new Thread(new ThreadForBatch(daoHashMap.get(tableName),batchManager.getCurrentPstmIndex()));
            thrd.start();

            if (isFinal){
                try {
                    thrd.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            //提交后切换 Pstm
            batchManager.switchPstmIndex();
        }
    }





}
