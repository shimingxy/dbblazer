/**
 * 
 */
package com.blazer.load.file.runner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.sql.DataSource;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.ConnUtil;
import com.blazer.db.TableColumns;


/**
 * 单数据文件导入
 * @author mhshi
 * 
 */
public class TransDataLoadFile{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataLoadFile.class);
	DataSource sourceDataSource;
	int  	commitNumber = 2000;
	int  	threadNumber =1;
	int	   limitTextSize=0;
	String tableName;
	String loadFilePath;
	String loadFileName;
	String fileNameSuffix;
	String terminatedString;
	String fileType="csv";
	boolean skipFirstRow=false;
	
	String fromUrl;
	String fromUser;
	ScriptEngineManager manager = new ScriptEngineManager();
	ScriptEngine engine = Float.valueOf(System.getProperty("java.specification.version"))<1.8? 
						  manager.getEngineByName("javascript"):manager.getEngineByName("nashorn");
	/**
	 * 转换代码 javascript 定义					  
	 */
	static String SCRIPT_CODE=
						"  	var columns[%s''];"//当前行的数据数组
						+ "	var dataValue='%s';"//当前列的数据
						+ "	var returnValue=''; "//定义返回值
						+ " %s ;";//用户定义javascript控制代码
	
	
	//<!-- FULL      在插入前进行删除,默认    -->
	//<!-- INCREMENT 先清除条件相关数据，然后按照条件进行增量插入 -->
	String transType="FULL";
	
	ArrayList<TableColumns> listTableColumns ;
	

	public TransDataLoadFile() {
		super();
		// TODO Auto-generated constructor stub
	}


	public TransDataLoadFile(DataSource sourceDataSource,int commitNumber, int threadNumber,
			String tableName, String loadFileName, String fileNameSuffix,
			String terminatedString, String selectSqlString, String exportFilePath,int	   limitTextSize) {
		super();
		this.commitNumber = commitNumber;
		this.threadNumber = threadNumber;
		this.tableName = tableName;
		this.loadFileName = loadFileName;
		this.fileNameSuffix = fileNameSuffix;
		this.terminatedString = terminatedString;
		this.sourceDataSource=sourceDataSource;
		this.limitTextSize=limitTextSize;
	}


	public void run() throws SQLException {
		//未填写数据列，则自动读取数据库内容
		if(listTableColumns==null) {
			buildMetaData(tableName);
		}
		
		if(fileType.equalsIgnoreCase("csv")) {
			runCsv();
		}else {
			runXlsx();
		}
		
	}
	
	public void runCsv() throws SQLException {
		Connection sourcConn=null;
		PreparedStatement pstmt=null;
		// TODO Auto-generated method stub
		try {
			String filePath=loadFilePath.replace("\\", "/")+""+loadFileName+fileNameSuffix;
			_logger.info("---------------thread "+threadNumber+" "+filePath);
			
			File txtFile=new File(filePath);
			if(txtFile.exists()&&txtFile.length()>0){//文件不存在或者为空的情况
				_logger.info("---------------thread "+threadNumber+" "+"file exists");
				InputStreamReader read=new InputStreamReader(new FileInputStream(filePath));
				BufferedReader bReader=new BufferedReader(read);
				String lineText;
				String insertSql=buildInsertSql();
				sourcConn=sourceDataSource.getConnection();
				sourcConn.setAutoCommit(false);
				pstmt=sourcConn.prepareStatement(insertSql);
				
				long insertNum=0;
				long commitCount=0;
				while((lineText=bReader.readLine())!=null){
					if(skipFirstRow&&commitCount==0){commitCount++ ;continue;}//跳过第一行
					lineText=lineText+" |+|";
					_logger.debug("---------------lineText "+lineText);
					String[] columnvalues=lineText.split("\\|\\+\\|");
					_logger.debug("---------------length "+columnvalues.length);
					
					for (String v :columnvalues) {
						_logger.debug("---------------column value "+v);
					}
					//TODO:
					/*
					if(columnvalues.length<listTableColumns.size()) {
						_logger.info("--------------- skip columnvalues "+columnvalues.length+" TableColumns "+listTableColumns.size());
						continue;
					}*/
					
					setValue(columnvalues,pstmt);
					
					commitCount++;
					
					insertNum += 1;
					if (insertNum >= this.commitNumber) {
						insertNum = 0;
						
						_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount );
						pstmt.executeBatch();
						sourcConn.commit();
					}
				}
				
				if (insertNum >0){
					_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount +" Complete .");
					pstmt.executeBatch();
					sourcConn.commit();
				}
				
				bReader.close();
			}else if(txtFile.length()<=0){
				_logger.info("---------------thread "+threadNumber+" "+"file length is 0");
			}else {
				_logger.info("---------------thread "+threadNumber+" "+"file not exists");
			}
		}catch(Exception e) {
			_logger.info("---------------thread "+threadNumber+" "+"Exception");
			_logger.info("---------------thread "+e.getMessage());
			 e.printStackTrace();
			 if(sourcConn!=null)sourcConn.rollback();
		}finally {
			ConnUtil.releaseConnection(sourcConn, null, pstmt, null);
		}
		
	}

	public void runXlsx() throws SQLException {
		Connection sourcConn=null;
		PreparedStatement pstmt=null;
		// TODO Auto-generated method stub
		try {
			String filePath=loadFilePath.replace("\\", "/")+""+loadFileName+fileNameSuffix;
			_logger.info("---------------thread "+threadNumber+" "+filePath);
			
			File excelFile=new File(filePath);
			if(excelFile.exists()&&excelFile.length()>0){
				_logger.info("---------------thread "+threadNumber+" "+"file exists");
				FileInputStream fis= new  FileInputStream(excelFile);
				Workbook workbook=null;
				//Workbook workbook=new HSSFWorkbook();
				if(fileNameSuffix.indexOf("xlsx")>-1) {
					workbook=new XSSFWorkbook(fis);
					_logger.info("--thread "+threadNumber+" XSSFWorkbook ...");
				}else {
					workbook=new HSSFWorkbook(fis);
					_logger.info("--thread "+threadNumber+" HSSFWorkbook ...");
				}
				Sheet sheet=workbook.getSheetAt(0);
				String insertSql=buildInsertSql();
				sourcConn=sourceDataSource.getConnection();
				sourcConn.setAutoCommit(false);
				pstmt=sourcConn.prepareStatement(insertSql);
				;
				long insertNum=0;
				long commitCount=0;
				for (Row rows : sheet) {
					if(skipFirstRow&&commitCount==0){commitCount++ ;continue;}//跳过第一行
					if(commitCount>sheet.getLastRowNum())break;
					String[] columnvalues=new String [listTableColumns.size()];
					_logger.debug("---------------length "+columnvalues.length);
					//读取每行记录各列的值
					for (int readColumn =0; readColumn<columnvalues.length;readColumn++) {
						columnvalues[readColumn]=rows.getCell(readColumn).toString();
						_logger.debug("---------------column value "+columnvalues[readColumn]);
					}
					
					setValue(columnvalues,pstmt);
					
					commitCount++;
					
					insertNum += 1;
					if (insertNum >= this.commitNumber) {
						insertNum = 0;
						
						_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount );
						pstmt.executeBatch();
						sourcConn.commit();
					}
				}
				
				if (insertNum >0){
					_logger.info("--thread "+threadNumber+"--Commit Count "+commitCount +" Complete .");
					pstmt.executeBatch();
					sourcConn.commit();
				}
				
				workbook.close();
				fis.close();
			}else if(excelFile.length()<=0){
				_logger.info("---------------thread "+threadNumber+" "+"file length is 0");
			}else {
				_logger.info("---------------thread "+threadNumber+" "+"file not exists");
			}
		}catch(Exception e) {
			_logger.info("---------------thread "+threadNumber+" "+"Exception");
			_logger.info("---------------thread "+e.getMessage());
			 e.printStackTrace();
			 if(sourcConn!=null)sourcConn.rollback();
		}finally {
			ConnUtil.releaseConnection(sourcConn, null, pstmt, null);
		}
		
	}
	public String buildInsertSql(){
		String sql="";
		String cols="";
		String vals="";
		int i=1;
		for(TableColumns tc : listTableColumns) {
			if(tc.isSkip()) {i++;continue;}//跳过
			if(i==1) {
				cols=tc.getColumnName();
				if(tc.isFixed()) {
					vals=" "+tc.getDefaultValue()+" ";
				}else {
					vals=" ? ";
				}
			}else {
				cols=cols+" , "+tc.getColumnName();
				if(tc.isFixed()) {
					vals=vals+" , "+tc.getDefaultValue()+" ";
				}else {
					vals=vals+" , ? ";
				}
			}
			_logger.debug("---------------thread "+threadNumber+" "+tc.getColumnName()+","+tc.getDataType());
			i++;
		}
		sql="INSERT INTO "+this.tableName +" ("+cols+") VALUES ("+vals+")";
		_logger.info("---------------thread "+threadNumber+" SQL "+sql);
		return sql;
	}
	
	public void setValue(String []columnvalues,PreparedStatement pstmt) throws Exception{
		//TODO:
		int pos=1;
		int columnPos=0;
		for(TableColumns tc :listTableColumns){
			if(tc.isSkip()||tc.isFixed()) {continue;}//跳过
			//TODO:
			String columnValue=columnvalues[columnPos++];
			if(tc.getConvert()!=null) {
				String columnsString="";
				for(String cv : columnvalues) {
					columnsString+="'"+cv+"',";
				}
				engine.eval(String.format(SCRIPT_CODE,columnsString,columnValue,tc.getConvert()));
				columnValue=engine.get("returnValue").toString();//获取返js回值
			}
			
			_logger.trace("--column "+tc.getColumnName()+" , "+tc.getDataType() +" , value "+columnValue);
			
			if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
					tc.getDataType().equalsIgnoreCase("VARCHAR")||
					tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
					tc.getDataType().equalsIgnoreCase("CHAR")||
					tc.getDataType().equalsIgnoreCase("RAW")
					){
				pstmt.setString(pos, columnValue.trim());
				
			}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
				if(tc.getDataLength()==22&&tc.getDataScale()>0){//NUMBER
					pstmt.setFloat(pos, Float.parseFloat(columnValue));
				}else if(tc.getDataLength()==22&&tc.getDataScale()==0){//INTEGER
					pstmt.setFloat(pos, Float.parseFloat(columnValue));
				}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){//LONG
					pstmt.setFloat(pos, Float.parseFloat(columnValue));
				}else{//DOUBLE
					pstmt.setFloat(pos, Float.parseFloat(columnValue));
				}
			}else if(tc.getDataType().equalsIgnoreCase("blob")){
				//stringBufferLines.append(getBlob(rs,tc.getColumnName()));
			}else if(tc.getDataType().equalsIgnoreCase("clob")||tc.getDataType().equalsIgnoreCase("NCLOB")){
				pstmt.setString(pos, columnValue);
			}else if(tc.getDataType().equalsIgnoreCase("DATE")){
				//stringBufferLines.append(rs.getDate(tc.getColumnName()));//targetPstmt.setDate(pos, rs.getDate(tc.getColumnName()));
			}else{
				pstmt.setString(pos, columnValue);
			}
			pos++;
			
		}	
		pstmt.addBatch();
		
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
	
	
	public void buildMetaData(String  tableName) throws SQLException{
		Connection sourcConn=sourceDataSource.getConnection();
		PreparedStatement pstmt=sourcConn.prepareStatement("SELECT * FROM "+tableName);
		buildMetaData(pstmt.getMetaData());
		pstmt.close();
		sourcConn.close();
	}
	
	
	public void buildMetaData(ResultSet rs) throws SQLException{
		ResultSetMetaData metaData = rs.getMetaData();
		buildMetaData(metaData);
	}
	
	public void buildMetaData(ResultSetMetaData metaData) throws SQLException{
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

	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}

	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}

	public int getCommitNumber() {
		return commitNumber;
	}

	public void setCommitNumber(int commitNumber) {
		this.commitNumber = commitNumber;
	}

	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}



	public int getLimitTextSize() {
		return limitTextSize;
	}


	public void setLimitTextSize(int limitTextSize) {
		this.limitTextSize = limitTextSize;
	}


	public String getFileNameSuffix() {
		return fileNameSuffix;
	}

	public void setFileNameSuffix(String fileNameSuffix) {
		this.fileNameSuffix = fileNameSuffix;
	}

	public String getTerminatedString() {
		return terminatedString;
	}

	public void setTerminatedString(String terminatedString) {
		this.terminatedString = terminatedString;
	}



	public ArrayList<TableColumns> getListTableColumns() {
		return listTableColumns;
	}

	public void setListTableColumns(ArrayList<TableColumns> listTableColumns) {
		this.listTableColumns = listTableColumns;
	}


	public String getLoadFileName() {
		return loadFileName;
	}


	public void setLoadFileName(String loadFileName) {
		this.loadFileName = loadFileName;
	}


	public String getLoadFilePath() {
		return loadFilePath;
	}


	public void setLoadFilePath(String loadFilePath) {
		this.loadFilePath = loadFilePath;
	}


	public int getThreadNumber() {
		return threadNumber;
	}


	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}


	public String getTransType() {
		return transType;
	}


	public void setTransType(String transType) {
		this.transType = transType;
	}


	public boolean isSkipFirstRow() {
		return skipFirstRow;
	}


	public void setSkipFirstRow(boolean skipFirstRow) {
		this.skipFirstRow = skipFirstRow;
	}


	public String getFileType() {
		return fileType;
	}


	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	

}
