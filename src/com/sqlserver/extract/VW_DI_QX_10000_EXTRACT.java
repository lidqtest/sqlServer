package com.sqlserver.extract;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sqlserver.utils.ColumnNumUtil;

public class VW_DI_QX_10000_EXTRACT {

	public static void main(String[] args) {
		String tableName=args[0];
		if(!("".equals(tableName))&&tableName!=null){
			tableName=tableName.toLowerCase();
		}
		System.out.println("tableName:"+tableName);
		List<String> tabList=new ArrayList<String>();
		tabList.add("int_evt_10000_vw_di_qx_billservtype");
		tabList.add("int_evt_10000_vw_di_qx_billtype");
		tabList.add("int_evt_10000_vw_di_qx_buzi_type");
		tabList.add("int_evt_10000_vw_di_qx_cc_callreason");
		tabList.add("int_evt_10000_vw_di_qx_customertype");
		tabList.add("int_evt_10000_vw_di_qx_servicetype");
		
		String dateNo=null;
		if(args.length>1){
			dateNo=args[1];
		}
		String url="jdbc:sqlserver://133.37.79.166:1433;DatabaseName=ccdw2";
		String user="zxdb_qx";
		String password="zxdb_qxqx";
		try{
			Map<String, String> paramMap=ColumnNumUtil.initParams("/data01/etl/liuhb/config.properties");
			String sql=paramMap.get(tableName);
			System.out.println("SQL:"+sql);
			if("".equals(sql)||sql==null){
				throw new NullPointerException("SQL不能为空");
			}
			int colLen=ColumnNumUtil.getColumnNumber(sql);
			
			String fileName=null;
			if(tabList.contains(tableName)){
				fileName=tableName+"_"+ColumnNumUtil.getCurrentTimestamp()+".txt";
			}else{
				fileName=tableName+"-date_no_-"+ColumnNumUtil.getPreDateNo()+"-"+ColumnNumUtil.getCurrentDateNo()+".txt";
			}
			System.out.println("fileName:"+fileName);
			File file=new File("/data01/fbk/10000/"+fileName);
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
