package ServerHelper.daoHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DaoManagerDistributer {
    private final int DAOPERSTUB = 3;
    public int curDaoIndex = 0;
    private int INSERTSPERDAO = 600000; //Distributer 负责让每个DaoMng获得指定数量的信息后，切换待命DaoMng


    private DaoManager[] mngArray = new DaoManager[DAOPERSTUB];

    public void prepareDao(String tableName,HashMap<String,Object> info){

        for (int i=0;i<DAOPERSTUB;i++){
            DaoManager mng = new DaoManager();
            mng.prepareDao(tableName,info);
            mngArray[i] = mng;
        }
    }

    synchronized public DaoManager getDaoManager(int count){

        int index = curDaoIndex;

        if (count>0 && count%INSERTSPERDAO == 0 ){
            curDaoIndex++;

            if (curDaoIndex == DAOPERSTUB){
                curDaoIndex = 0;
            }
        }

        return mngArray[index];
    }
}
