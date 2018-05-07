package ServerHelper;

import ServerHelper.daoHelper.DaoManagerDistributer;
import ServerHelper.daoHelper.ThreadForBatchInsert;
import dao.InfoDao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InfoManager {

    int TABLECOUNT = ArgsForTableAndBatch.activeTable;

    long infoCount = 0;
    long totalCount = 0;

    int bunble=0;
    int maxIndex=0;
    int curIndex=0;

    String[] tableNames;
    DaoManagerDistributer[] distributers;
    Thread[] threadHolder = new Thread[TABLECOUNT];


    public void init(Map map){

        tableNames = new String[TABLECOUNT];
        distributers = new DaoManagerDistributer[TABLECOUNT];

        for (int i=0; i<TABLECOUNT; i++){
            tableNames[i] = "tb_test" + i;
            distributers[i] = new DaoManagerDistributer();
            distributers[i].prepareDao(tableNames[i],map);

            InfoDao dao = distributers[i].getDaoManager().getDao();
            if (!dao.isTableExist(tableNames[i])){
                dao.createTable(tableNames[i]);
                System.out.println("created table" + tableNames[i]);
            }
        }


        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            Object key = entry.getKey();

            for (int i=0; i<TABLECOUNT; i++){
                InfoDao dao = distributers[i].getDaoManager().getDao();
                if(!dao.isColExist(key.toString(), tableNames[i])){
                    dao.addColumn(key.toString(),tableNames[i]);
                }
            }

        }
    }

    synchronized public long receiveInfo(InfoHolder[] info){

            // 获取空闲的table
            int index = getFreeThreadIndexByScroll();

            if (index>maxIndex) {

                maxIndex = index;
        //        System.out.println("=====" + maxIndex + "==== " );
            }
  //      System.out.println("------------------- " + bunble );
            Thread thrd = new Thread(new ThreadForBatchInsert(distributers[index].getDaoManager(),info,bunble));
            thrd.start();

            threadHolder[index] = thrd;

        bunble++;

        return 0;

    }

    public void complete(){
        try {
            for (Thread thrd:
                 threadHolder) {
                if (thrd!=null){
                    if (thrd.isAlive()){
                        thrd.join();
                    }
                }


            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int getFreeThreadIndexByRandom(){
        int index = -1;

        while (index == -1){
            int tmp = (int)(Math.random()*TABLECOUNT);

            if (threadHolder[tmp] == null){
                index = tmp;
            }
            else if (!threadHolder[tmp].isAlive()){
                index = tmp;
            }

        }



        return index;
    }

    private int getFreeThreadIndexByOrder(){

        int index = -1;

        while (index == -1){

            for (int i=0;i<threadHolder.length;i++){
                if (threadHolder[i] == null){
                    index = i;
                    break;
                }
                if (!threadHolder[i].isAlive()){
                    index = i;
                    break;
                }
            }
        }

        return index;
    }

    private int getFreeThreadIndexByScroll(){

        if (curIndex == TABLECOUNT) curIndex = 0;

        while (true){
            if (threadHolder[curIndex]==null){
                break;
            }
            if (!threadHolder[curIndex].isAlive()){
                break;
            }
        }

        return curIndex++;
    }

    public int getFreeThreadCount(){
        int count = TABLECOUNT;
        for (int i=0;i<TABLECOUNT;i++){
            if (threadHolder[i] != null){
                if (threadHolder[i].isAlive()){
                    count--;
                }
            }
        }
        return count;
    }

    public InfoDao getDao(){
        return distributers[0].getDaoManager().getDao();
    }

    public  long getTotalCount(){
        return totalCount;
    }
}
