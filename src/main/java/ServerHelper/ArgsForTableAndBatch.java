package ServerHelper;

public class ArgsForTableAndBatch {
    private static int resolve = 7 ;

    private static int[] table = {  100,  30,  60,   30,    60,    30,  80,     100,    40,    10,     10,     20};
    private static int[] batch = {2000,4000,4000,10000, 10000, 40000, 40000,  10000, 20000, 100000,  200000,  400000};
    //                             0     1   2     3      4      5      6       7      8      9      10      11

    public static int activeTable = table[resolve];
    public static int activeBatch = batch[resolve];
}
