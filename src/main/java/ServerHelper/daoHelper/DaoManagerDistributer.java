package ServerHelper.daoHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DaoManagerDistributer {
    private final int DAOPERSTUB = 1;
    private int curDaoIndex = 0;


    private DaoManager[] mngArray = new DaoManager[DAOPERSTUB];

    public void prepareDao(String tableName,HashMap<String,Object> info){

        for (int i=0;i<DAOPERSTUB;i++){
            DaoManager mng = new DaoManager();
            mng.prepareDao(tableName,info);
            mngArray[curDaoIndex] = mng;
        }
    }

    synchronized public DaoManager getDaoManager(){
        int index = curDaoIndex++;
        if (curDaoIndex == DAOPERSTUB){
            curDaoIndex = 0;
        }


        return mngArray[index];
    }
}
