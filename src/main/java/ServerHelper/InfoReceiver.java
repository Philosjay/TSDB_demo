package ServerHelper;

import ServerHelper.daoHelper.DaoManager;
import ServerHelper.daoHelper.DaoManagerDistributer;
import ServerHelper.daoHelper.ThreadForBatchInsert;
import io.grpc.dao.InfoMap;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfoReceiver {
    private int BUFFERSIZE = 10000;     //根据模块测试，数组长度为1000时插入效率最高，但跟其他模块配合时这个值不一定最优
    public int infoCount = 0;
    private int infoIndex = 0;



    public InfoReceiver(){

    }

    synchronized public void receiveInfo(Map<String,String> info, DaoManagerDistributer distr, String tableName){

        // info 存入定长Array 效率更高
//        infoArray[infoIndex] = new InfoHolder(info);
        infoCount++;
        infoIndex++;


        if (infoIndex == BUFFERSIZE){
//            releaseBuffer(distr.getDaoManager(infoCount),tableName,false);
        }

    }

    synchronized public void receiveInfoPacket(List<InfoMap> infoPacket, DaoManagerDistributer distr){

        // info 存入定长Array 效率更高
//        infoArray[infoIndex] = new InfoHolder(info);
        infoCount+= infoPacket.size();
        infoIndex+= infoPacket.size();

//        InfoHolder[] infoArray = new InfoHolder[infoPacket.size()];
//        for (int i=0;i<infoPacket.size();i++){
//            infoArray[i] = new InfoHolder(infoPacket.get(i).getInfoMapMap());
//
//        }


//        distr.getDaoManager().addInfoANDRequireBatchExcecution(infoArray);

//        Thread thrd = new Thread(new ThreadForBatchInsert(distr.getDaoManager(),infoArray));
//        thrd.start();


    }

    synchronized public void recForTest(Map<String,String> info){
//        infoArray[infoIndex] = new InfoHolder(info);
        infoCount++;
        infoIndex++;
        releaseBuffer(null,"",false);
    }

    synchronized public void releaseBuffer(DaoManager mng, String tableName, boolean isFinal){

        //开辟缓存信息插入pstm的线程
        if (infoIndex == BUFFERSIZE){
//            System.out.println("curDaoMng " + daoIndex);
//            Thread thrd = new Thread(new ThreadForBatchInsert(mng,infoArray,tableName,isFinal));
//            thrd.start();


            infoIndex = 0;
        }


    }

}
