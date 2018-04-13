package clientHelpers;

import org.hyperic.sigar.SigarException;
import utils.OSUtils;

import java.util.HashMap;
import java.util.List;

public class DiskInfoCollector extends InfoCollector {

    @Override
    public List<HashMap<String, Object>> getInfoHashList(){
        List<HashMap<String, Object>> list = null;
        try {
            list = OSUtils.getDiskMapList();
        } catch (SigarException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return list;
    }

    @Override
    public  HashMap<String, Object>filterInfo(HashMap<String, Object> map){
        // 把采集到的所有信息根据经过筛选，保留部分

        String devName = map.get("name").toString();

        /** 把设备名中的特殊字符替换, 否则无法在mysql 中创建 table **/
        if(devName.contains(":")){
            map.replace("name",devName.replace(":","_"));
        }else if(devName.contains("-")){
            map.replace("name",devName.replace(":","_"));
        }else if(devName.contains("/")){
            map.replace("name",devName.replace(":","_"));
        }


        return map;
    }
}
