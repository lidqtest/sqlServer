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

public class EXTERNAL_TABLE_PRODUCE {

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
		
		String fileName="external_table_script.txt";
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
			String sql="select 'create ' as aa, 'external table' as bb"+
					"   from dual"+
					" union all"+
					" select '' as aa, ' ext_q_"+tableName.toLowerCase()+"' as bb"+
					"   from dual"+
					" UNION ALL"+
					" SELECT '(rowkey ' as aa, 'string,' as bb"+
					"   from dual"+
					" UNION ALL"+
					" SELECT *"+
					"   FROM (SELECT COLUMN_NAME as aa, ' ' || HIVE_DATA_TYPE || ',' as bb"+
					"           FROM (SELECT A.COLUMN_NAME,"+
					"                        A.COLUMN_ID,"+
					"                        CASE"+
					"                          WHEN A.DATA_TYPE LIKE '%CHAR%' THEN"+
					"                           'STRING'"+
					"                          WHEN A.DATA_TYPE LIKE '%NUMBER%' and DATA_SCALE = 0 THEN"+
					"                           'BIGINT'"+
					"                          WHEN A.DATA_TYPE LIKE '%NUMBER%' and DATA_SCALE > 0 THEN"+
					"                           'DOUBLE'"+
					"                          WHEN A.DATA_TYPE LIKE '%DATE%' THEN"+
					"                           'TIMESTAMP'"+
					"                          WHEN (A.DATA_TYPE LIKE '%BLOB%' OR"+
					"                               A.DATA_TYPE LIKE '%CLOB%') THEN"+
					"                           'STRING'"+
					"                          ELSE"+
					"                           NULL"+
					"                        END AS HIVE_DATA_TYPE,"+
					"                        A.DATA_TYPE"+
					"                   FROM ALL_TAB_COLUMNS A, ALL_COL_COMMENTS B"+
					"                  WHERE A.TABLE_NAME = '"+tableName.toUpperCase()+"'"+
					"                    AND A.OWNER = '"+userName.toUpperCase()+"'"+
					"                    AND A.TABLE_NAME = B.TABLE_NAME(+)"+
					"                    AND A.OWNER = B.OWNER(+)"+
					"                    AND A.COLUMN_NAME = B.COLUMN_NAME(+)"+
					"                  ORDER BY A.COLUMN_ID)"+
					"         UNION ALL"+
					"         SELECT 'LOAD_TIME' as aa, ' TIMESTAMP' as bb"+
					"           FROM DUAL)"+
					" UNION ALL"+
					" select ')STORED BY ' as aa,"+
					"        '''org.apache.hadoop.hive.hbase.HBaseStorageHandler''' || ' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key' as bb"+
					"   from dual"+
					" UNION ALL"+
					" select tmp.aa,tmp.bb from (SELECT ',cf:' as aa,C.COLUMN_NAME as bb"+
					"   FROM ALL_TAB_COLUMNS C"+
					"  WHERE C.TABLE_NAME = '"+tableName.toUpperCase()+"'"+
					"    AND C.OWNER = '"+userName.toUpperCase()+"' order by C.COLUMN_ID)tmp"+
					" UNION ALL"+
					" SELECT ',cf:LOAD_TIME\")' as aa, '' as bb"+
					"   from dual"+
					" UNION ALL"+
					" SELECT 'TBLPROPERTIES' as aa,"+
					"        '(\"hbase.table.name\" = \"hb_int_new:q_"+tableName.toLowerCase()+"\",\"hbase.mapred.output.outputtable\" = \"hb_int_new:q_"+tableName.toLowerCase()+"\");' as bb"+
					"   FROM dual";
			
			System.out.println(sql);
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("start query");
			ResultSet rs=ps.executeQuery();
			System.out.println("query ok");
			StringBuffer content=new StringBuffer("");
			while(rs.next()){
				String col_val="";
				if(!("".equals(rs.getString(1)))&&!("null".equals(rs.getString(1)))&&rs.getString(1)!=null){
					if(rs.getString(1).contains("STORED BY")||rs.getString(1).indexOf("TBLPROPERTIES")>-1){
						col_val+=rs.getString(1);
					}else{
						col_val+=rs.getString(1).toLowerCase();
					}
				}
				if(!("".equals(rs.getString(2)))&&!("null".equals(rs.getString(2)))&&rs.getString(2)!=null){
					if(rs.getString(2).contains("org.apache.hadoop.hive.hbase.HBaseStorageHandler")){
						col_val+=rs.getString(2);
					}else{
						col_val+=rs.getString(2).toLowerCase();
					}
				}
				System.out.println(rs.getString(1)+"    "+rs.getString(2));
				content.append(col_val);
			}
			System.out.println("----------------------");
			System.out.println(content);
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
