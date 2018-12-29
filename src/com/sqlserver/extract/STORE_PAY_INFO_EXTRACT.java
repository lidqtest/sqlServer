package com.sqlserver.extract;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class STORE_PAY_INFO_EXTRACT {

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
			String sql=paramMap.get(tableName);
			System.out.println("SQL:"+sql);
			if("".equals(sql)||sql==null){
				throw new NullPointerException("SQL不能为空");
			}
			int colLen=ColumnNumUtil.getColumnNumber(sql);
			
			String fileName=tableName+"-date_no_-"+ColumnNumUtil.getPreDateNo()+"-"+ColumnNumUtil.getCurrentDateNo()+".txt";
			System.out.println("fileName:"+fileName);
			File file=new File("/data01/fbk/card/"+fileName);
			if(!file.exists()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			FileOutputStream fos=new FileOutputStream(file,true);
			OutputStreamWriter os=new OutputStreamWriter(fos,"UTF-8");
			System.out.println("load driver");
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("create connected");
			Connection conn=DriverManager.getConnection(url, user, password);
			System.out.println(conn);
			System.out.println("执行SQL："+sql);
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("start query");
			ResultSet rs=ps.executeQuery();
			System.out.println("query over");
			int index=0;
			StringBuffer content=new StringBuffer("");
			while(rs.next()){
				for(int i=1;i<=colLen;i++){
					if(i==colLen){
						content.append(rs.getString(i)).append("\n");
					}else{
						content.append(rs.getString(i)).append("\001");
					}
				}
				//写入文件
				if(index%5000==0&&index>0){
					os.write(content.toString());
					content=new StringBuffer("");
				}
				index++;
			}
			os.write(content.toString());
			if(rs!=null){
				rs.close();
			}
			if(ps!=null){
				ps.close();
			}
			if(conn!=null){
				conn.close();
			}
			if(os!=null){
				os.close();
			}
			if(fos!=null){
				fos.close();
			}
			System.out.println("game over");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
