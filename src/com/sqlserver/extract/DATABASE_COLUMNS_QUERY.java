package com.sqlserver.extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class DATABASE_COLUMNS_QUERY {

	public static void main(String[] args) throws Exception{
		String database=args[0];
		if(!("".equals(database))&&database!=null){
			database=database.toLowerCase();
		}
		System.out.println("databse:"+database);
		String tableName=args[1];
		if(!("".equals(tableName))&&tableName!=null){
			tableName=tableName.toLowerCase();
		}
		System.out.println("tableName:"+tableName);
		
		Map<String, String> jdbcParamMap=ColumnNumUtil.initParams("/data/ftp_dc/etl/jdbc.properties");
//		Map<String, String> jdbcParamMap=ColumnNumUtil.initParams("E:\\jdbc.properties");
		System.out.println(jdbcParamMap);
		String driver=jdbcParamMap.get(database+"_driver");
		String url=jdbcParamMap.get(database+"_url");
		String user=jdbcParamMap.get(database+"_user");
		String password=jdbcParamMap.get(database+"_password");
		System.out.println("********************************************");
		System.out.println("current driver:"+driver);
		System.out.println("current url:"+url);
		System.out.println("current user:"+user);
		System.out.println("current password:"+password);
		
		System.out.println("load driver");
		Class.forName(driver);
		System.out.println("create connecting");
		Connection conn=DriverManager.getConnection(url, user, password);
		System.out.println(conn);
		String sql="desc "+tableName;
		System.out.println("执行SQL："+sql);
		PreparedStatement ps=conn.prepareStatement(sql);
		System.out.println("start query");
		ResultSet rs=ps.executeQuery();
		System.out.println("query over");
		while(rs.next()){
			System.out.println(rs.getString("Field")+"    |    "+rs.getString("Type"));
		}
	}
}
