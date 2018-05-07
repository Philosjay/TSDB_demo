package clientHelpers;
import org.hyperic.sigar.SigarException;
import utils.OSUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfoCollector {
    Thread threadForUpdate;



    protected List<HashMap<String, Object>> list = null;


    public InfoCollector(){
        updateInfo();
    }

    public List<HashMap<String, Object>> getInfoHashList(){
        //Do nothing by default
        return null;
    }

    public HashMap<String, Object> filterInfo(HashMap<String, Object> map){
        // 把采集到的所有信息根据经过筛选，保留部分

        return map;
    }

    public Map<String,Object> getInfoMapByIndex(int index){
        updateInfo();

        return list.get(index);
    }


    public void updateInfo(){
        // 通过线程使update 的调用不被信息收集过程拖累


        if (threadForUpdate == null){
            threadForUpdate = new Thread(() -> {
                try {
                    list = OSUtils.getCpuPercMapList();
                } catch (SigarException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });

            threadForUpdate.start();
            try {
                threadForUpdate.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }else if(!threadForUpdate.isAlive()){
            threadForUpdate = new Thread(() -> {
                try {
                    list = OSUtils.getCpuPercMapList();
                } catch (SigarException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            threadForUpdate.start();
        }




    }
}
