package ServerHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InfoReceiver {
    private List<HashMap<String,Object>> infoList = new ArrayList<HashMap<String, Object>>() ;
    private long infoCount = 0;
    private DaoManager daoManager = null;

    public InfoReceiver(DaoManager mng){
        daoManager = mng;
    }

    public void addInfo(HashMap<String,Object> info){
        infoList.add(info);
        infoCount++;
    }

    public void requireDaoExcecution(String tableName, boolean isFinal){
        if (infoCount == 1000){

            Thread thrd = new Thread(new ThreadForBatchInsert(daoManager,infoList,tableName,isFinal));
            thrd.start();

            infoList = new ArrayList<HashMap<String, Object>>() ;
            infoCount = 0;
        }
    }

}
