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

public class MLOG_PRODUCE {

	public static final String DRIVER="oracle.jdbc.driver.OracleDriver";
	public static final String URL="jdbc:oracle:thin:@//133.37.117.211:1521/crm3int2";
	public static final String USER="datacenter";
	public static final String PASSWORD="Crmogg1q2w!";
	
	/*public static final String DRIVER="oracle.jdbc.driver.OracleDriver";
	public static final String URL="jdbc:oracle:thin:@133.37.253.176:1521:ORCL";
	public static final String USER="ogg";
	public static final String PASSWORD="yx!QAZ2wsx";*/
	
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
			String sql=" select 'create materialized view log on "+userName+"."+tableName+" tablespace tbs_mvlog_data with rowid,sequence(' from dual"+
					" union all"+
					" select case when COLUMN_ID<MAX_COLUMN_ID then COLUMN_NAME||',' else COLUMN_NAME end"+
					" from ("+
					" select b.COLUMN_NAME,b.COLUMN_ID,a.MAX_COLUMN_ID from "+
					" (select max(COLUMN_ID) MAX_COLUMN_ID FROM all_tab_columns A"+
					"   WHERE A.OWNER = upper('"+userName+"')"+
					"   AND A.TABLE_NAME = upper('"+tableName+"')"+
					"   )a,  "+
					" ("+
					" SELECT COLUMN_NAME,COLUMN_ID FROM all_tab_columns WHERE OWNER = upper('"+userName+"') AND TABLE_NAME = upper('"+tableName+"') order by COLUMN_ID"+
					" )b"+
					" )"+
					" union all"+
					" select ') including new values;' from dual";
			System.out.println(sql);
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("start query");
			ResultSet rs=ps.executeQuery();
			System.out.println("query ok");
			StringBuffer content=new StringBuffer("");
			while(rs.next()){
				content.append(rs.getString(1));
//				System.out.println(rs.getString(1));
			}
			System.out.println("----------------------");
//			System.out.println(content);
			content.append("\n");
			os.write(content.toString());
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
