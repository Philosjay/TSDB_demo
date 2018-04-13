package ServerHelper.daoHelper;

import dao.InfoDao;

import java.util.HashMap;
import java.util.Map;

public class DaoManager {
    private final int PSTM_PER_TABLE = 6;

    private InfoDao dao = null;
    private BatchExcecutionJudger batchExcecutionJudger = null;

    synchronized public void prepareDao(String tableName,HashMap<String,Object> info){


            dao = new InfoDao();
            dao.prepareConnection();

            //一个dao准备多个 PreparedStatement
            dao.prepareBatch(info,tableName,PSTM_PER_TABLE);
            batchExcecutionJudger = new BatchExcecutionJudger(PSTM_PER_TABLE);


    }

    synchronized public InfoDao getDao(){
        return dao;
    }


    synchronized public void addInfoANDRequireBatchExcecution(Map<String,Object> info){
        addInfoToBatch(info);
        requireBatchExcecution(false);
    }

    synchronized public void addInfoToBatch(Map<String,Object> info){


        dao.addInfoToBatch(info,batchExcecutionJudger.getCurrentPstmIndex());
        batchExcecutionJudger.countInsert();

    }

    synchronized public void requireBatchExcecution(boolean isFinal){


        if (isFinal || batchExcecutionJudger.toExcBatch()){
            Thread thrd = new Thread(new ThreadForBatchExcecution(dao,batchExcecutionJudger.getCurrentPstmIndex()));
            thrd.start();
//
//
//            if (isFinal){
//                try {
//                    thrd.join();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }

            //提交后切换 pstm
            batchExcecutionJudger.switchPstmIndex();
        }
    }
}
