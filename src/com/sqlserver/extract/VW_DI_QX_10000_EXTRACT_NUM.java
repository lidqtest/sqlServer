package com.sqlserver.extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class VW_DI_QX_10000_EXTRACT_NUM {

	public static void main(String[] args) {
		String tableName=args[0];
		if(!("".equals(tableName))&&tableName!=null){
			tableName=tableName.toLowerCase();
		}
		System.out.println("tableName:"+tableName);
		String dateNo=null;
		if(args.length>1){
			dateNo=args[1];
		}
		String url="jdbc:sqlserver://133.37.79.166:1433;DatabaseName=ccdw2";
		String user="zxdb_qx";
		String password="zxdb_qxqx";
		try{
			Map<String, String> paramMap=ColumnNumUtil.initParams("/data01/etl/liuhb/config.properties");
			String key=tableName+"_num";
			String sql=paramMap.get(key);
			System.out.println("SQL:"+sql);
			if("".equals(sql)||sql==null){
				throw new NullPointerException("SQL不能为空");
			}
			
			System.out.println("load driver");
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			System.out.println("create connected");
			Connection conn=DriverManager.getConnection(url, user, password);
			System.out.println(conn);
			if(!"".equals(dateNo)&&dateNo!=null){
				sql=sql+"'"+dateNo+"'";
			}
			System.out.println("执行SQL："+sql);
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
