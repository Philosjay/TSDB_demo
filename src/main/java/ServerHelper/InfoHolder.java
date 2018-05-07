package ServerHelper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InfoHolder {
    public Map<String,String> map;

    public InfoHolder(Map map){
        this.map = new HashMap<>();

        Iterator iterator = map.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry)iterator.next();
            Object key = entry.getKey();
            Object value = entry.getValue();

            this.map.put(key.toString(),value.toString());
        }
    }

}
