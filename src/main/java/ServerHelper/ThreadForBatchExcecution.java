package ServerHelper;

import dao.InfoDao;

public class ThreadForBatchExcecution implements Runnable {
    private InfoDao dao ;
    String pstmIndex;

    public ThreadForBatchExcecution(InfoDao dao, String pstmIndex){
        this.dao = dao;
        this.pstmIndex = pstmIndex;
    }

    @Override
    public void run() {
        dao.executeBatch(pstmIndex);
    }
}
