package services;

import org.hyperic.sigar.SigarException;
import utils.OSUtils;

import java.util.HashMap;
import java.util.List;

public class DiskInfoService extends InfoService {
	
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

}
