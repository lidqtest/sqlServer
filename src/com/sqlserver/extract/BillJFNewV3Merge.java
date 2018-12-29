package com.sqlserver.extract;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BillJFNewV3Merge {

	public static final int BUFSIZE = 1024 * 8;
	
	public static void main(String[] args) {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Calendar c=Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -1);
		String dateNo=sdf.format(c.getTime());
		
		String[] latnIds = {"028","0812", "0813", "0816", "0817", "0818", "0825", "0826", "0827", "0830", "0831", "0832", "0833", "0834", "0835", "0836", "0837", "0838", "0839", "0840", "0841"};
		for (String latnId : latnIds) {
			FilenameFilter filter = new BillJFNewV3FilenameFilterImpl(latnId);
			File dir = new File("/data01/fbk/bill/");
			String[] fileNames = dir.list(filter);
			System.out.println(latnId+"地市找到["+fileNames.length+"]个文件");
			if(fileNames.length>0){
				File[] files=dir.listFiles(filter);
				String outFile="int_act_bil_bill_new-date_no_-"+dateNo+"-latn_id_-"+latnId+"-"+sdf.format(new Date())+".txt";
				mergeFiles(outFile, fileNames);
				System.out.println("文件合并完成，开始删除子文件");
				for(File f : files){
					f.delete();
				}
			}else{
				System.out.println(latnId+"地市没有找到文件");
			}
		}
	}
	
	@SuppressWarnings("resource")
	public static void mergeFiles(String outFile, String[] files) {
		String path="/data01/fbk/bill/";
		FileChannel outChannel = null;
		try {
			outChannel = new FileOutputStream(path+outFile).getChannel();
			for (String f : files) {
				Charset charset = Charset.forName("utf-8");
				CharsetDecoder chdecoder = charset.newDecoder();
				CharsetEncoder chencoder = charset.newEncoder();
				FileChannel fc = new FileInputStream(path+f).getChannel();
				ByteBuffer bb = ByteBuffer.allocate(BUFSIZE);
				CharBuffer charBuffer = chdecoder.decode(bb);
				ByteBuffer nbuBuffer = chencoder.encode(charBuffer);
				while (fc.read(nbuBuffer) != -1) {
					bb.flip();
					nbuBuffer.flip();
					outChannel.write(nbuBuffer);
					bb.clear();
					nbuBuffer.clear();
				}
				fc.close();
			}
		} catch (Exception ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				if (outChannel != null) {
					outChannel.close();
				}
			} catch (Exception ignore) {
			}
		}
	}
}
class BillJFNewV3FilenameFilterImpl implements FilenameFilter {

	private String latnId;

	public BillJFNewV3FilenameFilterImpl(String latnId) {
		this.latnId = latnId;
	}

	@Override
	public boolean accept(File dir, String name) {
		if (name.startsWith("int_act_bil_bill_new_v3-date_no_") && name.endsWith(".txt") && name.contains(latnId)) {
			return true;
		}
		return false;
	}

}
