package ServerHelper.daoHelper;

import ServerHelper.InfoHolder;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadForBatchInsert implements Runnable{
    InfoHolder[] info;
    private DaoManager daoManager ;
    private int i ;

    public ThreadForBatchInsert(DaoManager mng, InfoHolder[] info){
        this.info = info;
        this.daoManager = mng;
    }

    public ThreadForBatchInsert(DaoManager mng, InfoHolder[] info,int index){
        this.info = info;
        this.daoManager = mng;
        i=index;
    }

    @Override
    public void run() {
            daoManager.batchExcecution(info);
   //         System.out.println("************************ " +  i + " complete");

    }

}
