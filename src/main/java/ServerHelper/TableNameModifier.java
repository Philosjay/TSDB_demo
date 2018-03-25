package ServerHelper;

public class TableNameModifier {
    public static String generateTableName(String devName, String userName){

        String tableName = "tb_" + devName;
        /** 把设备名中的特殊字符替换, 否则无法在mysql 中创建 table **/
        if(tableName.contains(":")){
            tableName = tableName.replace(":","_");
        }else if(tableName.contains("-")){
            tableName = tableName.replace("-","_");
        }else if(tableName.contains("/")){
            tableName = tableName.replace("/","_");
        }
        return  tableName + "_" + userName;
    }
}
