package com.sqlserver.extract;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sqlserver.utils.ColumnNumUtil;

public class EpayMonthConfigExtract {

	public static void main(String[] args) {
		String tableName=args[0];
		if(!("".equals(tableName))&&tableName!=null){
			tableName=tableName.toLowerCase();
		}else{
			throw new IllegalArgumentException("参数不能为空");
		}
		String url="jdbc:oracle:thin:@133.37.253.19:1521:EDWNEW2";
		String user="tbas";
		String password="c_bas_wtda2";
		try{
			List<String> list=new ArrayList<String>();
			String dateNo="20170301";
			SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
			Calendar c=Calendar.getInstance();
			while(!"20170430".equals(dateNo)){
				list.add(dateNo);
				c.setTime(sdf.parse(dateNo));
				c.add(Calendar.DAY_OF_MONTH, 1);
				dateNo=sdf.format(c.getTime());
			}
			list.add("20170430");
			for(String date : list){
				System.out.println(date);
			}
			//解析配置文件
			Map<String, String> paramMap=ColumnNumUtil.initParams("/data01/etl/liuhb/month_extract.properties");
			String sql=paramMap.get(tableName);
			if("".equals(sql)||sql==null){
				throw new IllegalArgumentException("查询SQL为空");
			}
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			ExecutorService threadPool=Executors.newFixedThreadPool(61);
			for(String date : list){
				threadPool.execute(new ExtractConfigDataRunnable(url, user, password, sdf, date, sql, tableName));
			}
			threadPool.shutdown();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
}
class ExtractConfigDataRunnable implements Runnable{

	private String url;
	private String user;
	private String password;
	private SimpleDateFormat sdf;
	private String dateNo;
	private String sql;
	private String tableName;
	public ExtractConfigDataRunnable(String url,String user,String password,SimpleDateFormat sdf,String dateNo,String sql,String tableName){
		this.url=url;
		this.user=user;
		this.password=password;
		this.sdf=sdf;
		this.dateNo=dateNo;
		this.sql=sql;
		this.tableName=tableName;
	}
	
	@Override
	public void run() {
		try{
			sql=sql+dateNo;
			System.out.println("执行SQL："+sql);
			int colLen=ColumnNumUtil.getColumnNumber(sql);
			
			System.out.println("create connection");
			Connection conn=DriverManager.getConnection(url, user, password);
			System.out.println("connected");
			String fileName=tableName+"-date_no_-"+dateNo+"-"+sdf.format(new Date())+".txt";
			System.out.println("fileName:"+fileName);
			File file=new File("/data01/fbk/epay/"+fileName);
			if(!file.exists()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			FileOutputStream fos=new FileOutputStream(file,true);
			OutputStreamWriter os=new OutputStreamWriter(fos,"UTF-8");
			PreparedStatement ps=conn.prepareStatement(sql);
			System.out.println("query...");
			ResultSet rs=ps.executeQuery();
			System.out.println("query over");
			int index=0;
			StringBuffer content=new StringBuffer("");
			while(rs.next()){
				for(int i=1;i<=colLen;i++){
					if(i==colLen){
						content.append(rs.getString(i)).append("\n");
					}else{
						content.append(rs.getString(i)).append("\005");
					}
				}
				//写入文件
				if(index%50000==0&&index>0){
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