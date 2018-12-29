package com.sqlserver.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ColumnNumUtil {

	public static int getColumnNumber(String sql){
		int count=0;
		try {
			CCJSqlParserManager parser = new CCJSqlParserManager();
			StringReader reader = new StringReader(sql);
			Statement stmt = parser.parse(reader);
			if (stmt instanceof Select) {
				Select selectStatement = (Select) stmt;
				SelectBody selectBody=selectStatement.getSelectBody();
				PlainSelect plainSelect=(PlainSelect) selectBody;
				List<SelectItem> selectItems=plainSelect.getSelectItems();
				count=selectItems.size();
				/*for(SelectItem item : selectItems){
					System.out.println(item);
				}*/
			}
		} catch (JSQLParserException e) {
			count=0;
			e.printStackTrace();
		}
		return count;
	}
	
	public static String getPreMonthNo(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMM");
		Calendar c=Calendar.getInstance();
		c.add(Calendar.MONTH, -1);
		String preMonthNo=sdf.format(c.getTime());
		return preMonthNo;
	}
	
	public static String getPreDateNo(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Calendar c=Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -1);
		String preDateNo=sdf.format(c.getTime());
		return preDateNo;
	}
	
	public static String getCurrentDateNo(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		return sdf.format(new Date());
	}
	
	public static String getCurrentMonthNo(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMM");
		return sdf.format(new Date());
	}
	
	public static String getCurrentTimestamp(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
		return sdf.format(new Date());
	}
	
	public static Map<String, String> initParams(String filePath) throws Exception{
		Map<String, String> paramMap = new HashMap<String, String>();
		InputStream in = new FileInputStream(filePath);
		InputStreamReader isr=new InputStreamReader(in,"UTF-8");
		Properties propConfig = new Properties();
		try {
			propConfig.load(isr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Set<Entry<Object, Object>> entrySetConf = propConfig.entrySet();
		for (Entry<Object, Object> entry : entrySetConf) {
			paramMap.put(entry.getKey().toString().trim().toLowerCase(), entry.getValue().toString().trim());
		}
		return paramMap;
	}
	
}
