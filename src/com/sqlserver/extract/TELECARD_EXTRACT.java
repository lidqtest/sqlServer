package com.sqlserver.extract;

import java.sql.Connection;
import java.sql.DriverManager;

public class TELECARD_EXTRACT {

	public static void main(String[] args) {
		String url = "jdbc:sqlserver://172.16.47.217:1433;DatabaseName=TeleCard_DataExchange";
		String user = "telecard_ods";
		String password = "telecard_ods";
		try {
			System.out.println("load driver");
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			System.out.println("create connected");
			Connection conn = DriverManager.getConnection(url, user, password);
			System.out.println(conn);
			System.out.println("game over");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
