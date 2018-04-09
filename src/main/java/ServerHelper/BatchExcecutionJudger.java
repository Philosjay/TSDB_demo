package ServerHelper;

public class BatchExcecutionJudger {
    private long insertsCount;
    private String currentPstmIndex;
    private final long INSERTS_PER_BATCH;
    boolean isFinalBatch;

    BatchExcecutionJudger(){
        insertsCount = 0;
        currentPstmIndex = "pstm0";
        isFinalBatch = false;
        INSERTS_PER_BATCH = 200000;
    }

    public void countInsert(){
        insertsCount++;
    }

    public String getCurrentPstmIndex() {
        return currentPstmIndex;
    }

    public boolean toExcBatch(){

        boolean toExc = false;
        if (insertsCount % (INSERTS_PER_BATCH) == 0 && insertsCount>0){
            toExc =true;
            insertsCount = 0;
        }



        return toExc;
    }

    public void switchPstmIndex(){
        if (currentPstmIndex == "pstm0"){
            currentPstmIndex = "pstm1";
        }else {
            currentPstmIndex = "pstm0";
        }
    }
}
