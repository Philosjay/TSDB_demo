package dao;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

import ServerHelper.InfoHolder;
import utils.JdbcUtils;

public class InfoDao {
	Connection[] con = null;
	PreparedStatement[] pstmArray = null;
	Statement stm = null;
	ResultSet rs =null;
	ResultSetMetaData rsmd = null;
	
	String sql_cols ;
	String sql_values ;

	private List<HashMap<String, Object>> generateMapListFromResultSet(ResultSet rs){

		List<HashMap<String,Object>> mapList = new ArrayList<HashMap<String, Object>>();
		try {
			rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			while(rs.next()){
				HashMap<String,Object> resultMap = new HashMap<String, Object>();
				for (int i=1; i<colCount+1; i++){
					resultMap.put(rsmd.getColumnLabel(i),rs.getObject(i));
				}
				mapList.add(resultMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return mapList;

	}

	public void prepareConnection(){
		try {
//			con = JdbcUtils.getConnection();

			con = new Connection[1];
			for (int i=0;i<1;i++){
				con[i] = JdbcUtils.getConnection();
				con[i].setAutoCommit(false);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closeConnection(){
		if (con != null) {
//			try {
//				con.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
		}
	}
	
	public void prepareBatch(Map<String, Object> info,String tableName,int pstmCount){
		
		//插入信息
		try {			
			//完善sql模板

			sql_cols = "insert into " + tableName  + "(";
			sql_values = "values (";
			for(int i=0; i< info.size(); i++){
				sql_values += "?";
				if(i<info.size()-1){
					sql_values += ",";
				}else{
					sql_values += ")";
				}
			}
			
			Map<String, Object> map = info;
			int i=0;
			for (Entry entry : map.entrySet()) {
				sql_cols += entry.getKey().toString();
				if(i<info.size()-1){
					sql_cols += ",";
				}else{
					sql_cols += ")";
				}
				i++;
			}

			pstmArray = new PreparedStatement[pstmCount];
			for (int j=0 ;j<pstmCount; j++){
//				PreparedStatement pstm = con.prepareStatement(sql_cols + sql_values);
				PreparedStatement pstm = con[0].prepareStatement(sql_cols + sql_values);
				pstmArray[j] = pstm;
			}

			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public void addInfoToBatch(InfoHolder[] info, int pstmIndex){


		//插入信息
		try {
		    for (int i=0;i<info.length;i++){
                PreparedStatement pstm = pstmArray[pstmIndex];
                //遍历HashMap，获得列名称
                int j =1;
                Iterator iter = info[i].map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry)iter.next();
                    Object val = entry.getValue();

                    pstm.setString(j, val.toString());
                    j++;
                }

                pstm.addBatch();
            }



		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	
	public void executeBatch(int pstmIndex){

//		System.out.println("                exc");
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

		try {
			PreparedStatement pstm = pstmArray[pstmIndex];
			pstm.executeBatch();
			con[0].commit();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public boolean isTableExist(String tableName) {
		// TODO Auto-generated method stub

		try{
			String sql = "SELECT COUNT(0) FROM information_schema.tables WHERE table_name = '" + tableName + "'";
			
			stm = con[0].createStatement();
			rs = stm.executeQuery(sql);
			boolean isExist= false;
			while(rs.next()){
				if(rs.getString("COUNT(0)").equals("1"))	isExist = true;
			}
			return isExist;
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
			
		}
	}
	
	public void createTable(String tableName){
		try{
			String sql = "CREATE table " + tableName;
			sql += "("
	//				+ "id INT UNSIGNED  PRIMARY KEY AUTO_INCREMENT ,"
					+ 	"time char(25) , "
					+	"name varchar(10) "
	//				+ " PRIMARY KEY (name, time)"
	//				+ "type varchar(50) "
					+ ")"
					+  "engine=MyISAM ";


//			sql += "("	+ "name varchar(50),"
//					+ 	"time TIMESTAMP, "
//					+ "type varchar(50) "
//					+ ")";
			stm = con[0].createStatement();
			stm.executeUpdate(sql);
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
			
		}
	}
	
	
	public boolean isColExist(String colName,String tableName){
		try{		
			
			stm = con[0].createStatement();
			
			String sql =  "select COUNT(0) from information_schema.columns WHERE table_name = '"+ tableName +  
					"' AND column_name = '" + colName + "'";
			rs = stm.executeQuery(sql);
			
			boolean isExist = false;
			while(rs.next()){
				if(rs.getString("COUNT(0)").equals("1")){
					isExist = true;
				}
			}
			
			return isExist;

			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
			
		}
	}
	
	public void addColumn(String colName,String tableName){
		
		try{						
			stm = con[0].createStatement();

			String sql = "ALTER TABLE " + tableName +" ADD column " +  colName + " float";
			stm.executeUpdate(sql);

		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
			
		}
	}

	public List<HashMap<String,Object>> findInfo(String tableName, HashMap<String,Object> infoMap){
		try{

			stm = con[0].createStatement();

			String sql =  "select * from " + tableName ;
			rs = stm.executeQuery(sql);

			List<HashMap<String,Object>> mapList = generateMapListFromResultSet(rs);

			return mapList;

		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}

		}
	}

	public void executeUpdate(String sql){
		try{
			stm = con[0].createStatement();
			stm.executeUpdate(sql);

		}catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			try {
				if(stm!=null)stm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}

		}
	}

    public List<HashMap<String,Object>> executeQuery(String sql){
        try{
            stm = con[0].createStatement();
            rs = stm.executeQuery(sql);

            List<HashMap<String,Object>> mapList = generateMapListFromResultSet(rs);
			rsmd = rs.getMetaData();
            return mapList;

        }catch(Exception e){
            throw new RuntimeException(e);
        }
        finally{
            try {
                if(stm!=null)stm.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }

        }
    }

    public int getRowCountInTable(String tableName){
        try{
            String sql = "select count(0) from " + tableName ;
            stm = con[0].createStatement();

            rs = stm.executeQuery(sql);

            int count=0;
            while (rs.next()){
                count = rs.getInt("count(0)");
            }

            List<HashMap<String,Object>> mapList = generateMapListFromResultSet(rs);
            rsmd = rs.getMetaData();


            return count;

        }catch(Exception e){
            throw new RuntimeException(e);
        }
        finally{
            try {
                if(stm!=null)stm.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }

        }
    }

}
