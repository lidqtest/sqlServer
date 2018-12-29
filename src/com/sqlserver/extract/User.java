package com.sqlserver.extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class User {
	
	public static final String DRIVER="oracle.jdbc.driver.OracleDriver";
	public static final String URL="jdbc:oracle:thin:@//133.37.117.211:1521/crm3int2";
	public static final String USER="datacenter";
	public static final String PASSWORD="Crmogg1q2w!";
	
	public static void main(String[] args) throws Exception {
		System.out.println("load driver");
		Class.forName(DRIVER);
		System.out.println("create connect...");
		Connection conn=DriverManager.getConnection(URL, USER, PASSWORD);
		
		String fileName="mlogs_script.txt";
		File file=new File("/home/dev/"+fileName);
		if(file.exists()){
			file.delete();
		}
		file.createNewFile();
		FileOutputStream fos=new FileOutputStream(file,true);
		OutputStreamWriter os=new OutputStreamWriter(fos,"UTF-8");
		
		File f=new File("/home/dev/conf.properties");
		InputStream is=new FileInputStream(f);
		InputStreamReader isr=new InputStreamReader(is);
		BufferedReader br=new BufferedReader(isr);
		String line=null;
		while((line=br.readLine())!=null){
			System.out.println(line);
			String[] tabs=line.split("\\.");
			String userName=tabs[0];
			String tableName=tabs[1];
			String sql="select * from all_tab_columns where owner='"+userName.toUpperCase()+"' and table_name='"+tableName.toUpperCase()+"' order by column_id";
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("start query");
			ResultSet rs=ps.executeQuery();
			System.out.println("query ok");
			String content="";
			while(rs.next()){
				content+=rs.getString(1);
				System.out.println(rs.getString(1));
			}
			System.out.println("----------------------");
			System.out.println(content);
			os.write(content);
			if(rs!=null){
				rs.close();
			}
			if(ps!=null){
				ps.close();
			}
		}
		if(br!=null){
			br.close();
		}
		if(isr!=null){
			isr.close();
		}
		if(is!=null){
			is.close();
		}
		if(conn!=null){
			conn.close();
		}
		if(os!=null){
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(fos!=null){
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("query over");
	}
}
