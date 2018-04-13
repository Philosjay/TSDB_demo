package ServerHelper.daoHelper;

import ServerHelper.InfoHolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadForBatchInsert implements Runnable{
    InfoHolder[] info;
    private DaoManager daoManager ;
    private String tableName;
    boolean isFinal;

    public ThreadForBatchInsert(DaoManager mng, InfoHolder[] info, String tableName, boolean isFinal){
        this.info = info;
        this.daoManager = mng;
        this.tableName = tableName;
        this.isFinal = isFinal;
    }

    @Override
    public void run() {
        for (InfoHolder info:
             info) {
            daoManager.addInfoANDRequireBatchExcecution(info.map);
        }

    }
}
