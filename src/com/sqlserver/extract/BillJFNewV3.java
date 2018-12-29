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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BillJFNewV3 {

    public static void main(String[] args) throws Exception{
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyMM");
        Date date=new Date();
        Calendar c=Calendar.getInstance();
        List<String> months=new ArrayList<String>();
        for(int i=-11;i<=0;i++){
            c.setTime(date);
            c.add(Calendar.MONTH, i);
            String month=sdf2.format(c.getTime());
            System.out.println(month);
            months.add(month);
        }
		
        String url="jdbc:oracle:thin:@//133.37.135.75:1521/acctchek";
        String user="dc_qry";
        String password="Dc_qr345";
        
        Map<String, String> latnIds=new HashMap<String, String>();
        latnIds.put("BACCT_LES", "0833");

        Calendar cc=Calendar.getInstance();
        cc.add(Calendar.DAY_OF_MONTH, -1);
        SimpleDateFormat sdf3=new SimpleDateFormat("yyyyMMdd");
        String dateNo=sdf3.format(cc.getTime());
        Class.forName("oracle.jdbc.driver.OracleDriver");
        ExecutorService threadPool=Executors.newFixedThreadPool(12);
        for(String latnId : latnIds.keySet()){
        	for(String month : months){
        		threadPool.execute(new ExtractBalancePayoutARunnable(url, user, password, month, sdf3, latnIds, latnId,dateNo));
        	}
        }
        threadPool.shutdown();
    }
}
class ExtractBalancePayoutARunnable implements Runnable{

    private String url;
    private String user;
    private String password;
    private String month;
    private SimpleDateFormat sdf;
    private Map<String, String> latnIds;
    private String latnId;
    private String dateNo;
    public ExtractBalancePayoutARunnable(String url,String user,String password,String month,SimpleDateFormat sdf,Map<String, String> latnIds,String latnId,String dateNo){
        this.url=url;
        this.user=user;
        this.password=password;
        this.month=month;
        this.sdf=sdf;
        this.latnIds=latnIds;
        this.latnId=latnId;
        this.dateNo=dateNo;
    }
    @Override
    public void run() {
        try{
            System.out.println("create connection");
            Connection conn=DriverManager.getConnection(url, user, password);
            String fileName="int_act_bil_bill_new_v3-date_no_-"+dateNo+"-latnId-"+latnIds.get(latnId)+"-"+sdf.format(new Date())+"_"+month+".txt";
            File file=new File("/data01/fbk/bill/"+fileName);
            if(!file.exists()){
                try {
                    file.createNewFile();
                    System.out.println("fileName not exists:"+fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                System.out.println("fileName exists:"+fileName);
            }
            FileOutputStream fos=new FileOutputStream(file,true);
            OutputStreamWriter os=new OutputStreamWriter(fos,"UTF-8");
            String sql=" select "+
                " BILL_ID,"+
                " PAYMENT_ID,"+
                " PAYMENT_METHOD,"+
                " BILLING_CYCLE_ID,"+
                " ACCT_ID,"+
                " STAFF_ID,"+
                " ORG_ID,"+
                " PROD_INST_ID,"+
                " AMOUNT,"+
                " LATE_FEE,"+
                " DERATE_LATE_FEE,"+
                " BALANCE,"+
                " LAST_CHANGE,"+
                " CUR_CHANGE,"+
                " to_char(CREATE_DATE,'yyyy-MM-dd hh24:mi:ss') as CREATE_DATE,"+
                " to_char(PAYMENT_DATE,'yyyy-MM-dd hh24:mi:ss') as PAYMENT_DATE,"+
                " replace(replace(replace(ACC_NUM,chr(13),''),chr(9),''),chr(10),'') as ACC_NUM,"+
                " OPERATED_BILL_ID,"+
                " STATUS_CD,"+
                " to_char(STATUS_DATE,'yyyy-MM-dd hh24:mi:ss') as STATUS_DATE,"+
                " to_char(sysdate,'yyyy-MM-dd hh24:mi:ss') load_time"+
                " from BACCT_LES.BILL_1"+month;
            PreparedStatement ps=conn.prepareStatement(sql);
            ResultSet rs=ps.executeQuery();
            System.out.println("query...");
            int index=0;
            StringBuffer content=new StringBuffer("");
            while(rs.next()){
                content.append(rs.getString("bill_id")).append("\001")
                .append(rs.getString("payment_id")).append("\001")
                .append(rs.getString("payment_method")).append("\001")
                .append(rs.getString("billing_cycle_id")).append("\001")
                .append(rs.getString("acct_id")).append("\001")
                .append(rs.getString("staff_id")).append("\001")
                .append(rs.getString("org_id")).append("\001")
                .append(rs.getString("prod_inst_id")).append("\001")
                .append(rs.getString("amount")).append("\001")
                .append(rs.getString("late_fee")).append("\001")
                .append(rs.getString("derate_late_fee")).append("\001")
                .append(rs.getString("balance")).append("\001")
                .append(rs.getString("last_change")).append("\001")
                .append(rs.getString("cur_change")).append("\001")
                .append(rs.getString("create_date")).append("\001")
                .append(rs.getString("payment_date")).append("\001")
                .append(rs.getString("acc_num")).append("\001")
                .append(rs.getString("operated_bill_id")).append("\001")
                .append(rs.getString("status_cd")).append("\001")
                .append(rs.getString("status_date")).append("\001")
                .append(rs.getString("load_time")).append("\n");
                
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
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
}
