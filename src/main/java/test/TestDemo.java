package test;


import ServerHelper.InfoReceiver;
import org.junit.Test;

import java.util.HashMap;

public class TestDemo {

    @Test
    public void testReceiver(){
        InfoReceiver receiver = new InfoReceiver();

        HashMap<String,Object> info = new HashMap<String, Object>();


        long start = System.currentTimeMillis();
        for (int i=0;i<10000000;i++){
            receiver.recForTest(info);
        }
        long end = System.currentTimeMillis();

        System.out.println((float)(end-start)/1000);

    }
}
