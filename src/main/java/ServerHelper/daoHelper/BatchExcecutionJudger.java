package ServerHelper.daoHelper;

import ServerHelper.ArgsForTableAndBatch;

public class BatchExcecutionJudger {
    private long insertsCount;
    private final long INSERTS_PER_BATCH;
    private final int PSTM_PER_TABLE ;
    boolean isFinalBatch;
    int pstmIndex;
    public int count=0;


    BatchExcecutionJudger(int pstmCount){
        insertsCount = 0;
        pstmIndex = 0;
        isFinalBatch = false;


        // 这两个参数的大小配合很重要，否则导致pstm 繁忙，信息丢失！
        INSERTS_PER_BATCH = ArgsForTableAndBatch.activeBatch;
        PSTM_PER_TABLE = pstmCount;
    }

    public void countInsert(int n){
        count+=n;
        insertsCount+=n;
    }

    public int getCurrentPstmIndex() {
        return pstmIndex;
    }

    public boolean toExcBatch(){

        boolean toExc = false;
        if (insertsCount >= INSERTS_PER_BATCH){
            toExc =true;
            insertsCount -= INSERTS_PER_BATCH;
        }



        return toExc;
    }

    public void switchPstmIndex(){

        pstmIndex++;
        if (pstmIndex == PSTM_PER_TABLE){
            pstmIndex =0;
        }
    }
}
