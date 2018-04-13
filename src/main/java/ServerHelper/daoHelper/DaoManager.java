package ServerHelper.daoHelper;

import ServerHelper.InfoHolder;
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


    synchronized public void addInfoANDRequireBatchExcecution(InfoHolder[] info){
//        addInfoToBatch(info);
//        requireBatchExcecution(false);


        dao.addInfoToBatch(info,batchExcecutionJudger.getCurrentPstmIndex());
        batchExcecutionJudger.countInsert(info.length);

        if ( batchExcecutionJudger.toExcBatch()){
//            Thread thrd = new Thread(new ThreadForBatchExcecution(dao,batchExcecutionJudger.getCurrentPstmIndex()));
//            thrd.start();

            dao.executeBatch(batchExcecutionJudger.getCurrentPstmIndex());
            //提交后切换待命pstm
            batchExcecutionJudger.switchPstmIndex();


        }
    }

    synchronized public void addInfoToBatch(InfoHolder[] info){


//        dao.addInfoToBatch(info,batchExcecutionJudger.getCurrentPstmIndex());
//        batchExcecutionJudger.countInsert();

    }

    synchronized public void requireBatchExcecution(boolean isFinal){

        //开辟pstm批量插入DB的线程
        if (isFinal || batchExcecutionJudger.toExcBatch()){
//            Thread thrd = new Thread(new ThreadForBatchExcecution(dao,batchExcecutionJudger.getCurrentPstmIndex()));
//            thrd.start();

            //提交后切换待命pstm
            batchExcecutionJudger.switchPstmIndex();
        }
    }
}
