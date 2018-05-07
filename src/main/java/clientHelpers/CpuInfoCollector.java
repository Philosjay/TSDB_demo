package clientHelpers;

import org.hyperic.sigar.SigarException;
import utils.OSUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CpuInfoCollector extends InfoCollector{



    @Override
    public List<HashMap<String, Object>> getInfoHashList(){

        updateInfo();

        try {
            list = OSUtils.getCpuPercMapList();
        } catch (SigarException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return list;
    }






    @Override
    public HashMap<String, Object> filterInfo(HashMap<String, Object> map){
        // 把采集到的所有信息根据经过筛选，保留部分

        map.remove("userUseRate");
        map.remove("sysUseRate");
        map.remove("waitRate");
        map.remove("errorRate");
        map.remove("idleRate");

    //    map.remove("time");
        map.remove("type");
   //     map.remove("name");


        return map;
    }

    public Map filterInfo(Map map){
        // 把采集到的所有信息根据经过筛选，保留部分

        map.remove("userUseRate");
        map.remove("sysUseRate");
        map.remove("waitRate");
        map.remove("errorRate");
        map.remove("idleRate");

        //    map.remove("time");
        map.remove("type");
        //     map.remove("name");


        return map;
    }

}
