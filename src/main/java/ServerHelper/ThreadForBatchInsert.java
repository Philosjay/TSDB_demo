package ServerHelper;

import dao.InfoDao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ThreadForBatchInsert implements Runnable{
    private List<HashMap<String,Object>> infoList = null ;
    private DaoManager daoManager ;
    private String tableName;
    boolean isFinal;

    public ThreadForBatchInsert(DaoManager mng, List<HashMap<String,Object>> infoList, String tableName, boolean isFinal){
        this.infoList = infoList;
        this.daoManager = mng;
        this.tableName = tableName;
        this.isFinal = isFinal;
    }

    @Override
    public void run() {
        for (HashMap<String,Object> map:
             infoList) {
            daoManager.addInfoToBatch(map,tableName);
            if (!isFinal){
                daoManager.requireBatchExcecution(tableName,false);
            }
        }

        if (isFinal){
            daoManager.requireBatchExcecution(tableName,true);
        }
    }
}
