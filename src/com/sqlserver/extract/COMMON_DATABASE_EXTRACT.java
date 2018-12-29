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
import java.util.Calendar;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class COMMON_DATABASE_EXTRACT {

	public static void main(String[] args) throws Exception {
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
		String preDateNo="";
		if(args.length>2){
			preDateNo=args[2];
			System.out.println("preDateNo:"+preDateNo);
		}
		
		Map<String, String> jdbcParamMap=ColumnNumUtil.initParams("/data01/etl/fengzj/jdbc.properties");
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
		
		Map<String, String> sqlParamMap=ColumnNumUtil.initParams("/data01/etl/fengzj/sql.properties");
//		Map<String, String> sqlParamMap=ColumnNumUtil.initParams("E:\\sql.properties");
		String sql0=sqlParamMap.get(tableName);
		System.out.println("Handle before SQL:"+sql0);
		if("".equals(sql0)||sql0==null){
			throw new NullPointerException("SQL不能为空");
		}
		String[] sqls=sql0.split("&");
		if(sqls.length<1){
			throw new IllegalArgumentException("SQL配置格式错误");
		}
		
		String isPar=sqls[0];
		String fileName=null;
		String sql=null;
		if("0".equals(isPar)){
			sql=sqls[1];
			fileName=tableName+"_"+ColumnNumUtil.getCurrentTimestamp()+".txt";
		}else if("1".equals(isPar)){
			String parType=sqls[1];
			sql=sqls[2];
			if("m".equals(parType)){
				SimpleDateFormat monthSDF=new SimpleDateFormat("yyyyMM");
				if(sql.contains("${yyyymm}")){
					sql=sql.replace("${yyyymm}", preDateNo);
				}else if(sql.contains("${yyyymm_+1}")){
					Calendar c=Calendar.getInstance();
					c.add(Calendar.MONTH, 1);
					String month=monthSDF.format(c.getTime());
					sql=sql.replace("${yyyymm_+1}", month);
				}else if(sql.contains("${yyyymm_1}")){
					sql=sql.replace("${yyyymm_1}", preDateNo);
				}
				if("".equals(preDateNo)||preDateNo==null){
					fileName=tableName+"-month_no_-"+ColumnNumUtil.getPreMonthNo()+"-"+ColumnNumUtil.getCurrentMonthNo()+".txt";
				}else{
					fileName=tableName+"-month_no_-"+preDateNo+"-"+ColumnNumUtil.getCurrentMonthNo()+".txt";
				}
			}else if("d".equals(parType)){
				SimpleDateFormat dateSDF=new SimpleDateFormat("yyyyMMdd");
				if(sql.contains("${yyyymmdd}")){
					sql=sql.replace("${yyyymmdd}", preDateNo);
				}else if(sql.contains("${yyyymmdd_+1}")){
					Calendar c=Calendar.getInstance();
					c.add(Calendar.DAY_OF_MONTH, 1);
					String dateNo=dateSDF.format(c.getTime());
					sql=sql.replace("${yyyymmdd_+1}", dateNo);
				}else if(sql.contains("${yyyymmdd_1}")){
					sql=sql.replace("${yyyymmdd_1}", preDateNo);
				}
				if("".equals(preDateNo)||preDateNo==null){
					fileName=tableName+"-date_no_-"+ColumnNumUtil.getPreDateNo()+"-"+ColumnNumUtil.getCurrentDateNo()+".txt";
				}else{
					fileName=tableName+"-date_no_-"+preDateNo+"-"+ColumnNumUtil.getCurrentDateNo()+".txt";
				}
			}
		}
		System.out.println("Handle after SQL:"+sql);
		int colLen=ColumnNumUtil.getColumnNumber(sql);
		
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
	}
}
