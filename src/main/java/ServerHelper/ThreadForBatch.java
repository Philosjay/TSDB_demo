package ServerHelper;

import dao.InfoDao;

public class ThreadForBatch implements Runnable {
    private InfoDao dao ;
    String pstmIndex;

    public ThreadForBatch(InfoDao dao, String pstmIndex){
        this.dao = dao;
        this.pstmIndex = pstmIndex;
    }

    @Override
    public void run() {
        dao.executeBatch(pstmIndex);
    }
}
