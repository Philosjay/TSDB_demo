package ServerHelper;

import dao.InfoDao;

import java.util.HashMap;

public class DaoManager {
    private HashMap<String, InfoDao> daoHashMap = new HashMap<String, InfoDao>();
    private HashMap<String, BatchExcecutionJudger> batchExcecutionJudgerMap= new HashMap<>();

    public void claimDao(String tableName, InfoDao dao){

        daoHashMap.put(tableName,dao);
        batchExcecutionJudgerMap.put(tableName,new BatchExcecutionJudger());
    }

    public InfoDao getDao(String tableName){
        return daoHashMap.get(tableName);
    }

    int count =0;
    public void addInfoToBatch(HashMap<String,Object> info, String tableName){

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


            //提交后切换 pstm
            batchManager.switchPstmIndex();
        }
    }





}
