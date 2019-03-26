/**
 * 
 */
package com.blazer.load.file;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.ConnUtil;
import com.blazer.load.file.runner.TransDataLoadFile;
import com.blazer.load.file.runner.TransDataLoadFileRunnable;
import com.blazer.pipeline.PipeLineTask;



/**
 * 多个文件的导入
 *  可以分多个线程进行导入
 * @author mhshi
 * 
 */
public class TransDataLoad implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataLoad.class);
	DataSource sourceDataSource;
	
	int threadSize = 1;
	
	String fromUrl;
	String fromUser;
	
	int    useFord;
	String loadFilePath;
	
	ArrayList <TransDataLoadFile>transDataLoadFileList;
	
	public int successThreadCount;
	public static int[] successThread = null;
	public static CountDownLatch mCountDownLatch = null;
	FileOutputStream fop = null;
	SimpleDateFormat sdf_ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	int fileNameUseSysdate = 0;

	public int execute() throws Exception {
		_logger.info("-- --From URL " + fromUrl+" , USER "+fromUser);
		
		String startTime=sdf_ymdhms.format(new Date());
		
		String envEtlDate= System.getProperty("ETL_DATE" );
	
		//替换日期
		if(envEtlDate!=null&&!envEtlDate.equals("")){
			if(fileNameUseSysdate==0) {
				//loadFileName=outFileName_a+envEtlDate+outFileName_c;
			}
		}else {
			/**
			 * TODO 
			 */
		}
		
		//文件名变换为小写
		//loadFileName=loadFileName.toLowerCase();
		//if(threadSize==5) return;
		//_logger.info("--thread  outFileName "+loadFileName+loadFileName+fileNameSuffix);
		
		if(beforeLoads()<0) return 1;
		
		int recordsCount=transDataLoadFileList.size();
		int threadRecordsCount=recordsCount/threadSize;
		_logger.info("--thread threadSize : "+threadSize);
		_logger.info("--thread csv  Count : "+(transDataLoadFileList.size())+" , threadFordsCount "+threadRecordsCount+" , "+(recordsCount%threadSize));
		mCountDownLatch=new CountDownLatch(recordsCount<threadSize?recordsCount:threadSize);
		successThread=new int[threadSize];
		if(recordsCount<threadSize) {
			for(int i=recordsCount;i<threadSize;i++) {
				_logger.info("thread skip "+i);
				successThread[i]++;
			}
		}
		
		ArrayList<TransDataLoadFile>  threadLoadFileList=new ArrayList<TransDataLoadFile>();
		long threadcount=0;
		int currThread=1;
		int count=1;
		//分配多个线程处理，一个线程处理多个文件
		
		for (TransDataLoadFile dataFile : transDataLoadFileList) {
			if(threadcount>=threadRecordsCount&&recordsCount>threadSize){
				_logger.info("threadcount "+currThread+" LoadFileList"+threadLoadFileList);
				runTranThread(currThread,threadLoadFileList);
				threadcount=0;
				currThread++;
				threadLoadFileList=new ArrayList<TransDataLoadFile>();
			}
			
			if(threadcount<threadRecordsCount&&currThread<threadSize) {
				_logger.info("count "+(count)+" ,table "+dataFile.getTableName());
				threadLoadFileList.add(dataFile);
				threadcount++;
				
			}else {
				_logger.info("count "+(count)+" ,table "+dataFile.getTableName());
				threadLoadFileList.add(dataFile);
			}
			count++;
		}
		
	   if(recordsCount%threadSize>0||recordsCount<threadSize) {
		   _logger.info("last threadcount "+currThread+" LoadCsvList"+threadLoadFileList);
		   runTranThread(currThread,threadLoadFileList);
	   }
		
	   mCountDownLatch.await();
		
		//File loadFilePathName = new File(loadFilePath+loadFileName+fileNameSuffix);
	   successThreadCount=0;
		for (int success : successThread) {
			if(success>=1) {
				successThreadCount++;
			}
		}
		
		_logger.info("-- --Start at " + startTime+" , End at  "+sdf_ymdhms.format(new Date())+" , Success Count "+successThreadCount+" , "+(successThreadCount>=threadSize ? 0 : 1));
		
		return successThreadCount>=threadSize ? 0 : 1;
		
	}
	
	public void  runTranThread(	int threadNumber,ArrayList <TransDataLoadFile>threadLoadFileList) {
		Runnable transThread =new TransDataLoadFileRunnable(
				   threadNumber,
				   threadLoadFileList);
		Thread tt=new Thread(transThread);
		tt.start();
	}
	
	public int beforeLoads()  {
		_logger.info("ETL_DATE "+System.getenv().get("ETL_DATE"));

		Connection conn=null;
		Statement stmt=null;
		try {
			conn=sourceDataSource.getConnection();
			stmt=conn.createStatement();
			//sqlBackupSource=sqlBackupSource.replaceAll("_ETL_DATE_", System.getenv().get("ETL_DATE"));
			//String []sqls=sqlBackupSource.split(";");
			for(TransDataLoadFile dataFile : transDataLoadFileList) {
				
				if(dataFile.getTransType().equalsIgnoreCase("FULL")) {
					String sql="TRUNCATE TABLE "+dataFile.getTableName();
					stmt.addBatch(sql);
					_logger.info("Execute Batch SQL \r\n"+sql+"");
				}
			}
			stmt.executeBatch();
			return 1;
		}catch(Exception e) {
			return -1;
		}finally {
			ConnUtil.releaseConnection(conn, stmt, null, null);
		}
		
		
	}
	

	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}

	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}



	public int getThreadSize() {
		return threadSize;
	}

	public void setThreadSize(int threadSize) {
		this.threadSize = threadSize;
	}

	public String getFromUrl() {
		return fromUrl;
	}

	public void setFromUrl(String fromUrl) {
		this.fromUrl = fromUrl;
	}

	public String getFromUser() {
		return fromUser;
	}

	public void setFromUser(String fromUser) {
		this.fromUser = fromUser;
	}

	public int getUseFord() {
		return useFord;
	}

	public void setUseFord(int useFord) {
		this.useFord = useFord;
	}

	public String getLoadFilePath() {
		return loadFilePath;
	}

	public void setLoadFilePath(String loadFilePath) {
		this.loadFilePath = loadFilePath;
	}

	public int getFileNameUseSysdate() {
		return fileNameUseSysdate;
	}

	public void setFileNameUseSysdate(int fileNameUseSysdate) {
		this.fileNameUseSysdate = fileNameUseSysdate;
	}

	public ArrayList<TransDataLoadFile> getTransDataLoadFileList() {
		return transDataLoadFileList;
	}

	public void setTransDataLoadFileList(ArrayList<TransDataLoadFile> transDataLoadFileList) {
		this.transDataLoadFileList = transDataLoadFileList;
	}

}
