package com.sqlserver.extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class STORE_PAY_INFO_EXTRACT_NUM {

	public static void main(String[] args) {
		String tableName=args[0];
		if(!("".equals(tableName))&&tableName!=null){
			tableName=tableName.toLowerCase();
		}
		System.out.println("tableName:"+tableName);
		
		String url="jdbc:mysql://10.183.7.21:3306/mer_db?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
		String user="eda_qry";
		String password="eda_qry";
		try{
			Map<String, String> paramMap=ColumnNumUtil.initParams("/data01/etl/liuhb/config_store.properties");
			String key=tableName+"_num";
			String sql=paramMap.get(key);
			System.out.println("SQL:"+sql);
			if("".equals(sql)||sql==null){
				throw new NullPointerException("SQL不能为空");
			}
			
			System.out.println("load driver");
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("create connect");
			Connection conn=DriverManager.getConnection(url, user, password);
			System.out.println("connected");
			System.out.println(conn);
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("start query");
			ResultSet rs=ps.executeQuery();
			System.out.println("query over");
			while(rs.next()){
				System.out.println("number is "+rs.getString("num"));
			}
			if(rs!=null){
				rs.close();
			}
			if(ps!=null){
				ps.close();
			}
			if(conn!=null){
				conn.close();
			}
			System.out.println("game over");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
