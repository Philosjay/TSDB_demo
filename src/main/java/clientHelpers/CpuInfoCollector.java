package clientHelpers;

import org.hyperic.sigar.SigarException;
import utils.OSUtils;

import java.util.HashMap;
import java.util.List;

public class CpuInfoCollector extends InfoCollector{
    @Override
    public List<HashMap<String, Object>> getInfoHashList(){
        List<HashMap<String, Object>> list = null;
        try {
            list = OSUtils.getCpuPercMapList();
        } catch (SigarException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return list;
    }

}
