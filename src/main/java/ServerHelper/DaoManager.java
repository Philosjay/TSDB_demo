package ServerHelper;

import dao.InfoDao;

import java.util.HashMap;

public class DaoManager {
    private HashMap<String, InfoDao> daoHashMap = new HashMap<String, InfoDao>();

    public void claimDao(String tableName, InfoDao dao){
        daoHashMap.put(tableName,dao);
    }

    public InfoDao getDao(String tableName){
        return daoHashMap.get(tableName);
    }
}
