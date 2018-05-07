package ServerHelper.daoHelper;

import java.util.HashMap;
import java.util.Map;

public class DaoManagerDistributer {
    private final int DAOPERSTUB = 1;
    public int curDaoIndex = 0;


    private DaoManager[] mngArray = new DaoManager[DAOPERSTUB];

    public void prepareDao(String tableName,Map<String,Object> info){

        for (int i=0;i<DAOPERSTUB;i++){
            DaoManager mng = new DaoManager();
            mng.prepareDao(tableName,info);
            mngArray[i] = mng;
        }
    }

    synchronized public DaoManager getDaoManager(){

        curDaoIndex++;

        if (curDaoIndex == DAOPERSTUB){
            curDaoIndex = 0;
        }

        return mngArray[curDaoIndex];
    }
}
