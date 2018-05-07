package test;

import ServerHelper.ArgsForTableAndBatch;
import ServerHelper.InfoHolder;
import ServerHelper.InfoManager;

import java.io.*;
import java.text.SimpleDateFormat;

public class ShellForTSDBControl {
    boolean isStarted = false;
    Thread threadForTSDB ;
    boolean isComplete = false;
    int freeThreadCount;
    long insertReceiced =0;
    boolean isInterrupted = false;
    String logPath = "/home/philosjay/TSDB/log/";

    public void printFreeThreadCount(){

        System.out.println("free thread count: " + freeThreadCount);
    }

    public void printInsertReceived(){
        System.out.println("insert received: " + insertReceiced);
    }



    public void run(){

        System.out.println("Shell started");

        String input ;

        do {
            input = getInput();

            if (!isStarted){

                runTSDB();

                System.out.println("TSDB started!");
                isStarted = true;
            }


            processInput(input);


        }while (!input.equals("quit"));

        System.out.println("waiting for DB...");
        try {
            threadForTSDB.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Bye!");

    }

    public String getInput(){
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String str = null;
        try {
            str = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return str;
    }

    public void processInput(String input){

         String[] args = input.split(" ");

         if (args[0].equals("status")){
             if (args[1].equals("-t")){
                 printFreeThreadCount();
             }
             else if(args[1].equals("-r")){
                 printInsertReceived();
             }
         }
         else if(args[0].equals("help")){
             System.out.println("=============Tip=============");
             System.out.println("status -t : Free threads");
             System.out.println("status -r: Insert received");
//             System.out.println("check  -c: Insert count");
             System.out.println("quit: Stop TSDB and quit shell");
             System.out.println(" ");
             System.out.println("log is saved to " + logPath);
             System.out.println("=============================");
         }
         else if (args[0].equals("quit")){
             isInterrupted = true;
         }


    }


    private void runTSDB(){

        threadForTSDB = new Thread(() -> {

            String logFileName;

            // 记录log
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
            BufferedWriter bw = null;

            logFileName = logPath +"log_" + df.format(System.currentTimeMillis());
            File file = new File(logFileName);
            FileWriter fw  = null;
            try {
                if (!file.exists()) {
                    file.createNewFile();
                }

                fw = new FileWriter(file.getAbsoluteFile());
                bw = new BufferedWriter(fw);
            } catch (IOException e) {
                e.printStackTrace();
            }



            InfoDuplicator dpl = new InfoDuplicator();
            InfoHolder[] info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);
            InfoManager infoMng = new InfoManager();
            infoMng.init(info[0].map);


            long start = System.currentTimeMillis();

            while (true){


                for (int i=0;i<200000/ArgsForTableAndBatch.activeBatch;i++){
                    infoMng.receiveInfo(info);

                    info = dpl.getInfos(ArgsForTableAndBatch.activeBatch);

                    insertReceiced += ArgsForTableAndBatch.activeBatch;
                }



                freeThreadCount = infoMng.getFreeThreadCount();
                long infoInDB =0;
                if (insertReceiced%12000000==0){
                    for (int i=0;i<ArgsForTableAndBatch.activeTable;i++){
                        infoInDB += infoMng.getDao().getRowCountInTable("tb_test" + i);
                    }

                }

                long end = System.currentTimeMillis();

                // 控制每一秒写入一次
                long insertTime = ( end - start);
                if (insertTime < 1000){
                    try {

                        Thread.sleep(1000 - insertTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


                end = System.currentTimeMillis();



                try {
                    bw.write("time " + df.format(System.currentTimeMillis()) + "\n");
                    if (infoInDB!=0) {
                        bw.write("insert received: " + insertReceiced +" while ");
                        bw.write("info in DB: " + infoInDB  + "\n");
                    }
                    bw.write("insert received: " + insertReceiced +"\n");
                    bw.write("free thread " + freeThreadCount  + "\n");

                    bw.write("_______________________________________________ " + (float)(end-start)/1000 + "\n");
                    bw.close();
                    fw.close();

                    file = new File(logFileName);

                    fw = new FileWriter(file.getAbsoluteFile(),true);
                    bw = new BufferedWriter(fw);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                start = System.currentTimeMillis();

                if (isInterrupted){
                    break;
                }
            }

            infoMng.complete();

            long end = System.currentTimeMillis();
            System.out.println((float)(end-start)/1000);

            isComplete = true;
        });


        threadForTSDB.start();

    }


}
