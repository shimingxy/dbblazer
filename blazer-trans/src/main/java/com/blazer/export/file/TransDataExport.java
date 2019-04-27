/**
 * 
 */
package com.blazer.export.file;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.export.file.runner.TransDataExport2Csv;
import com.blazer.export.file.runner.TransDataExport2Xlsx;
import com.blazer.pipeline.PipeLineTask;



/**
 * 数据数据导出到成CSV，XLSX文件<br>
 * XLSX文件为单线程导出<br>
 * CSV导出按记录数分多个线程<br>
 * 1、按照记录拆分为多个线程导出，每个线程导出一部分数据<br>
 * 2、生成操作系统合并命令，linux cat ,win copy<br>
 * 3、执行合并命令完成合并动作<br>
 * @author user
 * 
 */
public class TransDataExport extends BasicConfigure implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataExport.class);
	SimpleDateFormat sdf_ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	BasicConfigure basicConfigure;
	public int successThreadCount;
	/**
	 * -1 is error
	 * 0  no data 
	 * grater 0 data count
	 */
	public static long[] successThread = null;
	public static CountDownLatch mCountDownLatch = null;
	FileOutputStream fop = null;

	/**
	 * 初始化配置信息
	 * @throws  Exception 
	 */
	public void init() throws  Exception {
		_logger.info("-- --From URL " + fromUrl+" , USER "+fromUser);
		//读取默认配置
		if(basicConfigure!=null) {
			if(this.sourceDataSource==null&&basicConfigure.getSourceDataSource()!=null) {
				this.sourceDataSource=basicConfigure.getSourceDataSource();
				this.fromUrl=basicConfigure.getFromUrl();
				this.fromUser=basicConfigure.getFromUser();
			}
			
			if(this.commitNumber==0&& basicConfigure.getCommitNumber()>0) {
				this.commitNumber=basicConfigure.getCommitNumber();
			}
			if(this.limitTextSize==-1 &&basicConfigure.getLimitTextSize()>0) {
				this.limitTextSize=basicConfigure.getLimitTextSize();
			}
			if(this.threadSize==1&& basicConfigure.getThreadSize()>1) {
				this.threadSize=basicConfigure.getThreadSize();
			}
			
			if(this.terminatedString==null&&basicConfigure.getTerminatedString()!=null) {
				this.terminatedString=basicConfigure.getTerminatedString();
			}
			
			if(this.fileNameSuffix==null&&basicConfigure.getFileNameSuffix()!=null) {
				this.fileNameSuffix=basicConfigure.getFileNameSuffix();
			}
			if(this.exportFilePath==null&&basicConfigure.getExportFilePath()!=null) {
				this.exportFilePath=basicConfigure.getExportFilePath();
			}
		}
		
		if(this.outFileName==null) {
			this.outFileName=this.tableName.toLowerCase()+"_";
		}
		//文件名变换为小写
		outFileName=outFileName.toLowerCase();
		
		if(selectFieldsString==null||selectFieldsString.equals("")) {
			selectFieldsString="*";
		}
		if(this.whereSqlString==null) {
			this.whereSqlString="";
		}
		
		//如果没有配置字段信息，自动读取数据库默认字段信息
		buildMetaData(tableName);
		
		if(!fileType.equalsIgnoreCase("csv")) {
			threadSize=1;
		}
		
		
		String outFileName_a=outFileName;
		String outFileName_b="";
		String outFileName_c="";
		if(outFileName.indexOf("{")>0){
			 outFileName_a=outFileName.substring(0,outFileName.indexOf("{"));
			 outFileName_b=outFileName.substring(outFileName.indexOf("{")+1,outFileName.indexOf("}"));
			 outFileName_c=outFileName.substring(outFileName.indexOf("}")+1);
			_logger.info("--outFileName_a "+outFileName_a);
			_logger.info("--outFileName_b "+outFileName_b);
			_logger.info("--outFileName_c "+outFileName_c);
			SimpleDateFormat sdf =new SimpleDateFormat(outFileName_b);
			outFileName_b=sdf.format(new Date());
			outFileName=outFileName_a+outFileName_b+outFileName_c;
		}
		
		_logger.info("--outFileName "+outFileName);
		
		//exportFilePathName.createNewFile();
		String envSqlWhere=System.getProperty("SQL_WHERE");
		String envEtlDate= System.getProperty("ETL_DATE" );
		
		if(envSqlWhere!=null&&!envSqlWhere.equals("")){
			whereSqlString=whereSqlString+envSqlWhere;
		}
		
		//替换日期
		if(envEtlDate!=null&&!envEtlDate.equals("")){
			if(fileNameUseSysdate==0) {
				outFileName=outFileName_a+envEtlDate+outFileName_c;
			}
			whereSqlString=whereSqlString.replaceAll("_EXPORT_ETL_DATE_", envEtlDate);
		}else {
			/**
			 * TODO 
			 */
			whereSqlString=whereSqlString.replaceAll("_EXPORT_ETL_DATE_", outFileName_b);
		}
		
	}
	
	public int execute() throws Exception {
		String startTime=sdf_ymdhms.format(new Date());
		
		init();
		
		_logger.info("--thread  outFileName "+exportFilePath+outFileName+fileNameSuffix);
		File exportFilePathName = new File(exportFilePath+outFileName+fileNameSuffix);
		//清除当前文件
		if(exportFilePathName.exists()){
			exportFilePathName.delete();
		}
		
		_logger.info("--thread  sql "+("SELECT "+selectFieldsString+" FROM "+tableName+" "+whereSqlString));
		
		long recordsCount=0;
		if(whereSqlString!=null&&!whereSqlString.equals("")){
			recordsCount=getRecordCount(tableName+" "+whereSqlString);
		}else{
			recordsCount=getRecordCount(tableName);
			
		}
		if(recordsCount==0){
			_logger.info("-- --Start at " + startTime+" , End at  "+sdf_ymdhms.format(new Date()));
			_logger.info("-- --TABLE "+this.tableName +" has no data to export .");
			_logger.info("-- --Create Empty File "+exportFilePathName);
			exportFilePathName.createNewFile();
			return 0;
		}
		long threadRecordsCount=recordsCount/threadSize;
		_logger.info("--thread recordsCount : "+(recordsCount)+" , threadRecordsCount "+threadRecordsCount+" , "+(recordsCount%threadSize));
		mCountDownLatch=new CountDownLatch(threadSize);
		successThread=new long[threadSize];
		
		long startRow=0;
		long endRow=0;
		String threadSql="";
		for(int i=1;i<=threadSize;i++){
			successThread[i-1]=-1;
			if(threadSize!=i){
				endRow=endRow+threadRecordsCount;
				//_logger.info("--thread "+i+" recordsCount : "+(threadRecordsCount));
			}else{
				//_logger.info("--thread "+i+" recordsCount : "+(threadRecordsCount+recordsCount%threadSize));
				endRow=endRow+threadRecordsCount+recordsCount%threadSize;
			}
			_logger.info("--thread "+i+" threadRecordsCount : "+(endRow-startRow)+" , start Row "+startRow+" , end Row "+endRow);
			//当前为Oracle定制
			if(whereSqlString!=null&&!whereSqlString.equals("")){
				threadSql="SELECT "+selectFieldsString +" FROM (SELECT "+selectFieldsString +" , ROWNUM RN FROM "+tableName+" SEL_TEMP_TABLE "+whereSqlString+") WHERE RN > "+startRow+" AND RN <= "+endRow+"";
			}else{
				threadSql="SELECT "+selectFieldsString +" FROM (SELECT "+selectFieldsString +" , ROWNUM RN FROM "+tableName+" SEL_TEMP_TABLE ) WHERE RN > "+startRow+" AND RN <= "+endRow+"";
			}
			runTranThread(sourceDataSource,threadSql,i);
			_logger.info("--thread "+i+" thread Sql : "+threadSql);
			startRow=endRow;
		}
		mCountDownLatch.await();
		
		if(merge&&this.fileType.equalsIgnoreCase("csv")) {//合并文件标识
			if(System.getenv("OS").toUpperCase().startsWith("Windows".toUpperCase())){
				windowsMerge();
			}else{
				unixMerge();
			}
		}
		
		successThreadCount=0;
		long commitDataCount=0;
		for (long successCount : successThread) {
			if(successCount>=0) {//error is -1
				successThreadCount++;
				commitDataCount=commitDataCount+successCount;
			}
		}
		_logger.info("--Success Count "+commitDataCount);
		_logger.info("-- --Start at " + startTime+" , End at  "+sdf_ymdhms.format(new Date())+" , Success Count "+successThreadCount+" , "+(successThreadCount>=threadSize ? 0 : 1));
		return successThreadCount>=threadSize ? 0 : 1;
		
	}
	
	public void windowsMerge() throws Exception {
		String sfile="";
		//copy file
		for(int i=1;i<=threadSize;i++){
			if(i!=threadSize){
				sfile=sfile+outFileName+"_"+i+fileNameSuffix +"+";
			}else{
				sfile=sfile+outFileName+"_"+i+fileNameSuffix +"";
			}
		}
		String shCmdString=exportFilePath+outFileName+".bat";
		File shFilePathName = new File(shCmdString);
		if(shFilePathName.exists()){
			shFilePathName.delete();
		}
		shFilePathName.createNewFile();
		this.fop = new FileOutputStream(shFilePathName);
		fop.write((""+exportFilePath.substring(0,2)+"\r\r\n").getBytes());
		fop.write(("cd "+exportFilePath+"\r\r\n").getBytes());
		fop.write(("copy  "+sfile+"  "+outFileName+fileNameSuffix+"\r\r\n").getBytes());
		_logger.info("-- copy  : "+"copy "+sfile+"  "+outFileName+fileNameSuffix);
		
		//fop.write(("del /f/s/q "+shCmdString+"\r\n").getBytes());
		
		fop.close();
		
		Process proc=Runtime.getRuntime().exec(""+shCmdString);
		proc.waitFor();
		Thread.sleep(5000);
		for(int i=1;i<=threadSize;i++){
			//fop.write(("del /f/s/q  "+outFileName+"_"+i+fileNameSuffix +"\r\r\n").getBytes());
			//Runtime.getRuntime().exec("del /f/s/q  "+exportFilePath+outFileName+"_"+i+fileNameSuffix );
			Thread.sleep(1000);
			(new File(exportFilePath+outFileName+"_"+i+fileNameSuffix)).deleteOnExit();
			
			_logger.info("-- delete file  : "+exportFilePath+outFileName+"_"+i+fileNameSuffix);
		}
		
		Thread.sleep(1000);
		_logger.info("-- delete cmd file  : "+shCmdString);
		(new File(shCmdString)).deleteOnExit();
		
		//_logger.info("-- delete file  : "+"cmd /c del /f/s/q "+shCmdString);
		//Runtime.getRuntime().exec("del /f/s/q "+shCmdString);
	}
	
	public void unixMerge() throws  Exception {
		String sfile="";
		//cat file
		for(int i=1;i<=threadSize;i++){
			sfile=sfile+exportFilePath+outFileName+"_"+i+fileNameSuffix +" ";
		}
		String shCmdString=exportFilePath+outFileName+".sh";
		File shFilePathName = new File(shCmdString);
		if(shFilePathName.exists()){
			shFilePathName.delete();
		}
		shFilePathName.createNewFile();
		this.fop = new FileOutputStream(shFilePathName);
		fop.write(("cat "+sfile+" > "+exportFilePath+outFileName+fileNameSuffix+"\n").getBytes());
		_logger.info("-- cat  : "+"cat "+sfile+" > "+exportFilePath+outFileName+fileNameSuffix);
		
		for(int i=1;i<=threadSize;i++){
			fop.write(("rm -fr "+exportFilePath+outFileName+"_"+i+fileNameSuffix +"\n").getBytes());
			_logger.info("-- delete file  : "+"rm -fr "+exportFilePath+outFileName+"_"+i+fileNameSuffix);
		}
		fop.close();
		
		Process proc=Runtime.getRuntime().exec("sh "+shCmdString);
		proc.waitFor();
		Thread.sleep(5000);
		proc=Runtime.getRuntime().exec("rm -fr "+shCmdString );
		proc.waitFor();
	}
	
	public void  runTranThread(	DataSource sourceDataSource,String threadSql,int threadNumber) {
		Runnable transThread =null;
		if(this.fileType.equalsIgnoreCase("csv")||this.fileType.equalsIgnoreCase("txt")) {
			transThread =new TransDataExport2Csv(sourceDataSource,
					 commitNumber,  threadNumber,
					 tableName,  outFileName,  fileNameSuffix,
					 terminatedString,  threadSql,  exportFilePath,limitTextSize);
		}else {
		
			transThread =new TransDataExport2Xlsx(sourceDataSource,
					 commitNumber,  threadNumber,
					 tableName,  outFileName,  fileNameSuffix,
					 terminatedString,  threadSql,  exportFilePath,limitTextSize);
		}
		
		Thread tt=new Thread(transThread);
		tt.start();
	}
	/**
	 * 记录数查询，为分页做准备
	 * @param tableQuery
	 * @return
	 * @throws SQLException
	 */
	public long getRecordCount(String tableQuery) throws SQLException{
        String countSql="SELECT COUNT(*) COUNTROWS  FROM "+tableQuery;
        //_logger.debug("--thread "+threadNum+" countSql : "+countSql);
        Connection conn=sourceDataSource.getConnection();
		Statement  sourcStmt = conn.createStatement();
        ResultSet  contResultSet = sourcStmt.executeQuery(countSql);
        //insertRecordsCount=0;
        long recordsCount = 0;
        while(contResultSet.next()){
        	recordsCount = contResultSet.getLong(1); 
        }
        contResultSet.close();
        sourcStmt.close();
        conn.close();
        _logger.info("--recordsCount : "+recordsCount);
        return recordsCount;
	}
	
	/**
	 * 元数据查询
	 * @param tableQuery
	 * @throws SQLException
	 */
	public void buildMetaData(String tableQuery) throws SQLException{
		if(this.selectFieldsString.indexOf("*")>-1) {
			Connection conn=this.sourceDataSource.getConnection();
			PreparedStatement pstmt=conn.prepareStatement("SELECT * FROM "+tableQuery);
			ResultSetMetaData metaData = pstmt.getMetaData();
			_logger.debug("--buildMetaData --column Count "+metaData.getColumnCount() );
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if(i==1) {
					selectFieldsString=metaData.getColumnName(i);
				}else {
					selectFieldsString=selectFieldsString+" , "+metaData.getColumnName(i);
				}
				_logger.debug("-- buildMetaData --No. "+i+" , Column "+metaData.getColumnName(i)+" , DataType "+metaData.getColumnTypeName(i) );

			}
		}
	}

	public BasicConfigure getBasicConfigure() {
		return basicConfigure;
	}

	public void setBasicConfigure(BasicConfigure basicConfigure) {
		this.basicConfigure = basicConfigure;
	}


}
