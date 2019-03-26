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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.export.file.BasicConfigure;
import com.blazer.export.file.TransDataExport;


/**
 * @author mhshi
 * 
 */
public class TransDataExport2Xlsx  extends BasicConfigure  implements Runnable{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataExport2Xlsx.class);
	String selectSqlString;
	int  	threadNumber =1;
	FileOutputStream fop = null;
	
	ArrayList<TableColumns> listTableColumns ;
	

	public TransDataExport2Xlsx(DataSource sourceDataSource,int commitNumber, int threadNumber,
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
			File exportFilePathName = new File(exportFilePath+outFileName+fileNameSuffix);

			try {
				if(exportFilePathName.exists()){
					exportFilePathName.delete();
				}
				//exportFilePathName.createNewFile();
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
				//fop.close();
				
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
		TransDataExport.successThread[threadNumber-1]++;
		_logger.info("--thread "+threadNumber+" status "+TransDataExport.successThread[threadNumber-1]+" -- Complete .");
	}
	
	public void batchWrite(ResultSet rs) throws Exception{
		Workbook workbook=null;
		//Workbook workbook=new HSSFWorkbook();
		if(fileNameSuffix.indexOf("xlsx")>-1) {
			workbook=new XSSFWorkbook();
			_logger.info("--thread "+threadNumber+" XSSFWorkbook ...");
		}else {
			workbook=new HSSFWorkbook();
			_logger.info("--thread "+threadNumber+" HSSFWorkbook ...");
		}
		Sheet sheet=workbook.createSheet(tableName);

		long insertNum=0;
		Integer commitCount=0;
		Row row =sheet.createRow(0);
		for(int ccount=0;ccount<listTableColumns.size();ccount++){
			TableColumns tc =listTableColumns.get(ccount);
			Cell cell=row.createCell(ccount);
			cell.setCellValue(
					tc.getColumnComments()!=null &&!tc.getColumnComments().equals("")?
							tc.getColumnComments():
							tc.getColumnName());
			
		}
		
		while(rs.next()){
			row =sheet.createRow(commitCount + 1);
			for(int ccount=0;ccount<listTableColumns.size();ccount++){
				TableColumns tc =listTableColumns.get(ccount);
				Cell cell=row.createCell(ccount);
				//_logger.info("--column "+tc.getColumnName()+" , "+tc.getDataType() );
				if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
						tc.getDataType().equalsIgnoreCase("VARCHAR")||
						tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
						tc.getDataType().equalsIgnoreCase("CHAR")||
						tc.getDataType().equalsIgnoreCase("RAW")
						){
					//_logger.info("==="+tc.getColumnName());
					cell.setCellValue(rs.getString(tc.getColumnName()));
					
										
				}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
					if(tc.getDataLength()==22&&tc.getDataScale()>0){//NUMBER
						cell.setCellValue(rs.getLong(tc.getColumnName()));
						//targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else if(tc.getDataLength()==22&&tc.getDataScale()==0){//INTEGER
						cell.setCellValue(rs.getInt(tc.getColumnName()));
						//targetPstmt.setInt(pos, rs.getInt(tc.getColumnName()));
					}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){//LONG
						cell.setCellValue(rs.getLong(tc.getColumnName()));
						//targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else{//DOUBLE
						cell.setCellValue(rs.getDouble(tc.getColumnName()));
						//targetPstmt.setDouble(pos, rs.getDouble(tc.getColumnName()));
					}
				}else if(tc.getDataType().equalsIgnoreCase("blob")){
					cell.setCellValue(getBlob(rs,tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("clob")||tc.getDataType().equalsIgnoreCase("NCLOB")){
					String lobString =getClob(rs,tc.getColumnName());
					//_logger.info(lobString);
					if(0<this.limitTextSize && limitTextSize<lobString.length())lobString=lobString.substring(0, limitTextSize);
					cell.setCellValue(lobString==null?"":lobString.replaceAll("\r", "<CR><LF>").replaceAll("\n", "<CR><LF>"));
				}else if(tc.getDataType().equalsIgnoreCase("DATE")){
					cell.setCellValue(rs.getDate(tc.getColumnName()));//targetPstmt.setDate(pos, rs.getDate(tc.getColumnName()));
				}else{
					cell.setCellValue(rs.getObject(tc.getColumnName()).toString());
				}
			}
			
			commitCount++;
			
			insertNum += 1;
			if (insertNum >= this.commitNumber) {
				insertNum = 0;
				_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount );
			}
		}
		if (insertNum >0){
			_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount +" Complete .");
			
		}
		if(commitCount>0) {
			workbook.write(fop);
			fop.close();
			//workbook.d
			workbook.close();
			_logger.info("--thread "+threadNumber+"--Write to File Complete .");
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
