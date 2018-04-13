package ServerHelper.daoHelper;

public class BatchExcecutionJudger {
    private long insertsCount;
    private int currentPstmIndex;
    private final long INSERTS_PER_BATCH;
    private final int PSTM_PER_TABLE ;
    boolean isFinalBatch;
    int pstmIndex;
    public int count=0;


    BatchExcecutionJudger(int pstmCount){
        insertsCount = 0;
        pstmIndex = 0;
        currentPstmIndex = 0;
        isFinalBatch = false;


        // 这两个参数的大小配合很重要，否则导致pstm 繁忙，信息丢失！
        INSERTS_PER_BATCH = 200000;
        PSTM_PER_TABLE = pstmCount;
    }

    public void countInsert(int n){
        count+=n;
        insertsCount+=n;
    }

    public int getCurrentPstmIndex() {
        return currentPstmIndex;
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
        pstmIndex = pstmIndex % (PSTM_PER_TABLE);

        currentPstmIndex =  pstmIndex;
    }
}