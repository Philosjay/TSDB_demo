package testUnits;

import java.util.ArrayList;
import java.util.List;

public class ClientManager {
    private static int clientCount = 20;


    public static void main(String[] args){

        List<Thread> thrdList = new ArrayList<>();

        for (int i=1; i<=clientCount; i++){
            Thread clientThrd = new Thread(new Client("localhost", 50051,"Client" + i));
            thrdList.add(clientThrd);
        }


        long startTime=System.currentTimeMillis();//记录开始时间
        for (Thread thrd:
                thrdList) {
            thrd.start();
        }

        for (Thread thrd:
             thrdList) {
            try {
                thrd.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long endTime=System.currentTimeMillis();//记录结束时间
        float excTime=(float)(endTime-startTime)/1000;
        System.out.println(excTime);



    }

}
