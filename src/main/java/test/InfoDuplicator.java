package test;

import ServerHelper.ArgsForTableAndBatch;
import ServerHelper.InfoHolder;
import clientHelpers.CpuInfoCollector;
import java.util.Map;

public class InfoDuplicator {

    CpuInfoCollector collector = new CpuInfoCollector();


    public InfoHolder[] getInfos(int bunbleSize){
        collector.updateInfo();
        InfoHolder[] infos = new InfoHolder[bunbleSize];

        int count = 0;

        while (true){

            collector.updateInfo();
            for (int i=0;i<8;i++){

                Map<String,Object> map = collector.getInfoMapByIndex(i);
                map = collector.filterInfo(map);

                infos[count++] = new InfoHolder(map);

                if (count ==bunbleSize){
                    break;
                }

            }

            if (count == bunbleSize){
                break;
            }


        }



        return infos;

    }


}
