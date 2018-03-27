package dao;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

import utils.JdbcUtils;

public class InfoDao {
	Connection con=null;
	PreparedStatement pstm = null;
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
			con = JdbcUtils.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closeConnection(){
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void prepareBatch(HashMap<String, Object> info,String tableName){
		
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

			int i=0;
			for (Entry entry : info.entrySet()) {
				sql_cols += entry.getKey().toString();
				if(i<info.size()-1){
					sql_cols += ",";
				}else{
					sql_cols += ")";
				}
				i++;
			}
			
			pstm = con.prepareStatement(sql_cols + sql_values);	
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public void addInfoToBatch(HashMap<String, Object> info,String tableName){
		
		//插入信息
		try {			

			//遍历HashMap，获得列名称
			int i =1;
			Iterator iter = info.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry)iter.next();
				Object val = entry.getValue();

				pstm.setString(i, val.toString());
				i++;
			}
			
			pstm.addBatch();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	public void addInfo(HashMap<String, Object> info,String tableName){

		//插入信息
		prepareBatch(info, tableName);
		addInfoToBatch(info, tableName);
		executeBatch();
	}

	public void addInfo_withoutBatch(HashMap<String, Object> info,String tableName){

		try{
			String sql_colName = "INSERT INTO " + tableName + "(" ;
			String sql_values = "VALUES (" ;

			int count =1;
			int size = info.size();
			Iterator iter = info.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry)iter.next();
				Object val = entry.getValue();
				Object key = entry.getKey();



				sql_colName += key.toString();
				sql_values += "'" + val.toString() + "'";
				if(count < size){
					sql_colName += ", ";
					sql_values += ", ";
				}
				count++;
			}
			sql_colName += ")";
			sql_values += ")";

			String sql = sql_colName + sql_values;

			stm = con.createStatement();
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
	
	public void executeBatch(){
		try {
			pstm.executeBatch();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(pstm!=null)	pstm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
			
		}
	}
	
	
	public boolean isTableExist(String tableName) {
		// TODO Auto-generated method stub

		try{
			String sql = "SELECT COUNT(0) FROM information_schema.tables WHERE table_name = '" + tableName + "'";
			
			stm = con.createStatement();		
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
			sql += "(" + "id INT UNSIGNED  PRIMARY KEY AUTO_INCREMENT,"
					+	"name varchar(50),"
					+ 	"time TIMESTAMP  "
					+ ")";
			stm = con.createStatement();		
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
			
			stm = con.createStatement();
			
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
			stm = con.createStatement();

			String sql = "ALTER TABLE " + tableName +" ADD column " +  colName + " double";
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

			stm = con.createStatement();

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
			stm = con.createStatement();
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
            stm = con.createStatement();
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

}
