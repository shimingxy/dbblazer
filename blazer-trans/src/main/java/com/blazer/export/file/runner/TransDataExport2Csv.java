/**
 * 
 */
package com.blazer.export.file.runner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.export.file.BasicConfigure;
import com.blazer.export.file.TransDataExport;


/**
 * @author mhshi
 * 
 */
public class TransDataExport2Csv extends BasicConfigure implements Runnable{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataExport2Csv.class);

	String selectSqlString;
	int  	threadNumber =1;
	FileOutputStream fop = null;
	
	long commitCount=0;
	
	ArrayList<TableColumns> listTableColumns ;
	

	public TransDataExport2Csv(DataSource sourceDataSource,int commitNumber, int threadNumber,
			String tableName, String outFileName, String fileNameSuffix,
			String terminatedString, String selectSqlString, String exportFilePath,int	   limitTextSize) {
		super();
		this.commitNumber = commitNumber;
		this.threadNumber = threadNumber;
		this.tableName = tableName;
		this.outFileName = outFileName;
		this.fileNameSuffix = fileNameSuffix;
		this.terminatedString = terminatedString;
		this.selectSqlString = selectSqlString;
		this.exportFilePath = exportFilePath;
		this.sourceDataSource=sourceDataSource;
		this.limitTextSize=limitTextSize;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
			File exportFilePathName = new File(exportFilePath+outFileName+"_"+threadNumber+fileNameSuffix);

			try {
				if(exportFilePathName.exists()){
					exportFilePathName.delete();
				}
				exportFilePathName.createNewFile();
				this.fop = new FileOutputStream(exportFilePathName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			listTableColumns = new ArrayList<TableColumns>();
			try {
				Connection sourcConn=sourceDataSource.getConnection();
				Statement  sourcStmt = sourcConn.createStatement();
				ResultSet  sourceRs=sourcStmt.executeQuery(selectSqlString);
				buildMetaData(sourceRs);
				batchWrite(sourceRs);
				sourceRs.close();
				sourcStmt.close();
				sourcConn.close();
				fop.close();
				
				Thread.sleep(2000);
				
				successThread();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				_logger.info("--thread "+threadNumber+" -- Fail .");
				_logger.error("导出数据错误", e);
			}
		
		TransDataExport.mCountDownLatch.countDown();
	}
	
	//TODO
	public  synchronized void successThread() {
		TransDataExport.successThread[threadNumber-1]=commitCount;
		_logger.info("--thread "+threadNumber+" data count "+TransDataExport.successThread[threadNumber-1]+" -- Complete .");
	}
	
	public void batchWrite(ResultSet rs) throws Exception{

		long insertNum=0;
		
		StringBuffer stringBufferLines=new StringBuffer("");
		while(rs.next()){
			for(int ccount=0;ccount<listTableColumns.size();ccount++){
				TableColumns tc =listTableColumns.get(ccount);
				//_logger.info("--column "+tc.getColumnName()+" , "+tc.getDataType() );
				if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
						tc.getDataType().equalsIgnoreCase("VARCHAR")||
						tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
						tc.getDataType().equalsIgnoreCase("CHAR")||
						tc.getDataType().equalsIgnoreCase("RAW")
						){
					//_logger.info("==="+tc.getColumnName());
					String strColumnValue=rs.getString(tc.getColumnName());
					//长度大于limitTextSize，进行截取
					if(strColumnValue!=null &&0<this.limitTextSize &&limitTextSize<strColumnValue.length()) {
						strColumnValue=strColumnValue.substring(0, limitTextSize);
					}
					//回车、换行替换成ASCII char 26/SUB,在导入到数据库中需要替换成char13
					stringBufferLines.append(strColumnValue==null?"":strColumnValue.trim().replaceAll("\r", (char)26+"").replaceAll("\n", (char)(26)+""));
										
				}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
					if(tc.getDataLength()==22&&tc.getDataScale()>0){//NUMBER
						stringBufferLines.append(rs.getLong(tc.getColumnName()));
						//targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else if(tc.getDataLength()==22&&tc.getDataScale()==0){//INTEGER
						stringBufferLines.append(rs.getInt(tc.getColumnName()));
						//targetPstmt.setInt(pos, rs.getInt(tc.getColumnName()));
					}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){//LONG
						stringBufferLines.append(rs.getLong(tc.getColumnName()));
						//targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else{//DOUBLE
						stringBufferLines.append(rs.getDouble(tc.getColumnName()));
						//targetPstmt.setDouble(pos, rs.getDouble(tc.getColumnName()));
					}
				}else if(tc.getDataType().equalsIgnoreCase("blob")){
					stringBufferLines.append(getBlob(rs,tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("clob")||tc.getDataType().equalsIgnoreCase("NCLOB")){
					String lobString =getClob(rs,tc.getColumnName());
					//_logger.info(lobString);
					if(0<this.limitTextSize && limitTextSize<lobString.length())lobString=lobString.substring(0, limitTextSize);
					stringBufferLines.append(lobString==null?"":lobString.replaceAll("\r", (char)(26)+"").replaceAll("\n", (char)26+""));
				}else if(tc.getDataType().equalsIgnoreCase("DATE")){
					stringBufferLines.append(rs.getDate(tc.getColumnName()));//targetPstmt.setDate(pos, rs.getDate(tc.getColumnName()));
				}else{
					stringBufferLines.append(rs.getObject(tc.getColumnName()));
				}
				
				if(ccount!=listTableColumns.size()-1){
					stringBufferLines.append(terminatedString);
				}else{
					stringBufferLines.append("\r\n");
				}
			}
			
			commitCount++;
			
			insertNum += 1;
			if (insertNum >= this.commitNumber) {
				insertNum = 0;
				_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount );
				fop.write(stringBufferLines.toString().getBytes());
				stringBufferLines=new StringBuffer("");
			}
		}
		if (insertNum >0){
			_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount +" Complete .");
			fop.write(stringBufferLines.toString().getBytes());
		}
		
	}
	
	public String getClob(ResultSet sourceRs,String column) throws Exception{
		oracle.sql.CLOB sb=(oracle.sql.CLOB)sourceRs.getClob(column);
		if(sb==null)return "";
		Reader is=sb.getCharacterStream();
		char[]data=new char[(int)sb.length()];
		is.read(data);
		is.close();
		return new String(data);
	}
	
	public String  getBlob(ResultSet sourceRs,String column) throws Exception{
		oracle.sql.BLOB sb=(oracle.sql.BLOB)sourceRs.getBlob(column);
		if(sb==null)return "";
		InputStream is=sb.getBinaryStream();
		byte[]data=new byte[(int)sb.length()];
		is.read(data);
		is.close();
		return new String(data);	
	}
	public void buildMetaData(ResultSet rs) throws SQLException{
		ResultSetMetaData metaData = rs.getMetaData();
		_logger.debug("--thread "+threadNumber+"--column Count "+metaData.getColumnCount() );
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			TableColumns tc=new TableColumns();
			tc.setColumnName(metaData.getColumnName(i));
			tc.setDataType(metaData.getColumnTypeName(i));
			tc.setTableName(metaData.getTableName(i));
			tc.setDataPrecision(metaData.getPrecision(i));
			tc.setDataScale(metaData.getScale(i));
			_logger.debug("--thread "+threadNumber+"--No. "+i+" , Column "+tc.getColumnName()+" , DataType "+tc.getDataType() );
			listTableColumns.add(tc);
		}
	}


	

}
