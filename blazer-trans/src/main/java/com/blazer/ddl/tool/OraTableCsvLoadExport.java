package com.blazer.ddl.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.pipeline.PipeLineTask;


public class OraTableCsvLoadExport  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(OraTableCsvLoadExport.class);
	FileOutputStream out;
	String url;
	String user;
	String pass;
	String driverClass;
	DataSource dataSource;
	String owner;
	
	String configFilePath;
	String exportFilePath;
	Connection conn;
	String terminatedString;
	String inFilePath;
	String badFilePath;
	String fileNameSuffix="_${etl_flag}_000_000";
	
	@Override
	public int execute() throws Exception {
		// TODO Auto-generated method stub
		conn = dataSource.getConnection();
		
		_logger.info("-- --FROM SERVER " + this.url + "\n");
		_logger.info("-- --FROM USER " + this.user + "\n");
		_logger.info("-- --TO FILE " + this.exportFilePath + "\n");
		readExportData(configFilePath);

		conn.close();
		return 0;
	}
	
	public void  readTable(String tableName) throws Exception{
		//out.write("--Create Table \r\n".getBytes());
		Statement stmt=conn.createStatement();
		/*String sql="select t.owner,t.table_name,c.COMMENTS from sys.all_all_tables t,sys.all_tab_comments c "+
					" where t.table_name=c.TABLE_NAME "+
					" and t.table_name='"+tableName+"' and t.owner ='"+owner+"' "+
					" order by t.owner,t.table_name";*/
		String sql="select t.owner,t.table_name,c.COMMENTS as TABLECOMMENTS,col.COLUMN_NAME,"+
			" col.DATA_TYPE,col.DATA_LENGTH,col.DATA_SCALE,col.DATA_PRECISION,colc.COMMENTS AS COLUMNCOMMENTS ,col.NULLABLE,col.DATA_DEFAULT"+
			" from sys.all_all_tables t,sys.all_tab_comments c,sys.all_tab_columns col,sys.all_col_comments colc "+
			" where t.table_name=c.TABLE_NAME "+
			" and col.TABLE_NAME=t.table_name "+
			" and colc.TABLE_NAME=t.table_name "+
			" and col.COLUMN_NAME=colc.COLUMN_NAME "+
			" and t.owner ='"+owner+"' "+
			" and c.owner ='"+owner+"' "+
			" and col.owner ='"+owner+"' "+
			" and colc.owner ='"+owner+"' "+
			" and upper(t.table_name)='"+tableName.toUpperCase()+"' "+
			" order by t.owner,t.table_name,col.COLUMN_ID";
		
		_logger.info(sql);
		ArrayList<TableColumns> tcArray=new ArrayList<TableColumns>();
		ResultSet rs=stmt.executeQuery(sql);
		
		while(rs.next()){
			_logger.info("COLUMN_NAME : "+rs.getString("COLUMN_NAME"));
			TableColumns tc=new TableColumns();
			tc.setOwner(rs.getString("owner"));
			tc.setTableName(rs.getString("table_name"));
			tc.setTableComments(rs.getString("TABLECOMMENTS"));
			tc.setColumnName(rs.getString("COLUMN_NAME"));
			tc.setColumnComments(rs.getString("COLUMNCOMMENTS"));
			tc.setDataType(rs.getString("DATA_TYPE"));
			tc.setDataLength(rs.getInt("DATA_LENGTH"));
			tc.setDataScale(rs.getInt("DATA_SCALE"));
			tc.setDataPrecision(rs.getInt("DATA_PRECISION"));
			tc.setNullAble(rs.getString("NULLABLE"));
			tc.setDefaultValue(rs.getString("DATA_DEFAULT"));
			//DATA_DEFAULT
			//col.NULLABLE
			
			
			tcArray.add(tc);
		}
		int count=1;
		
		for(TableColumns tc : tcArray){
			out.write(("			<bean class=\"com.db.TableColumns\">\r\n").getBytes());
			out.write(("				<property name=\"columnName\" value=\""+tc.getColumnName()+"\"/>\r\n").getBytes());
			out.write(("				<property name=\"dataType\" value=\""+tc.getDataType().toUpperCase()+"\"/>\r\n").getBytes());
			out.write(("			</bean>\r\n").getBytes());
			count++;
			_logger.debug("-- --No." + count + " , columnName : " + tc.getColumnName() +" , dataType "+tc.getDataType().toUpperCase()+ "\r\n");
		}
		
		
		rs.close();
		stmt.close();
		
	}
	
	public void  readExportData(String filePath) throws Exception{
		File txtFile=new File(filePath);
		File exportFile=new File(exportFilePath);
		if(exportFile.exists()){
			exportFile.delete();
		}
		exportFile.createNewFile();
		out=new FileOutputStream(exportFile);
		out.write(("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n").getBytes());
		ArrayList<String >tableList =new ArrayList<String >();
		if(txtFile.exists()){
			InputStreamReader read=new InputStreamReader(new FileInputStream(filePath));
			BufferedReader bReader=new BufferedReader(read);
			String lineText;
			int sourceCount=0;
			while((lineText=bReader.readLine())!=null){
				if(lineText.startsWith("#")){
					//锟斤拷锟�?
				}else if(lineText.startsWith("--")){
					
				}else if(lineText.startsWith("++")){
					
				}else{
					_logger.info(lineText);
					tableList.add("transDataLoad_"+lineText.substring(2).toLowerCase());
					sourceCount++;
					out.write(("<!-- No. "+sourceCount +" TABLE "+lineText.toUpperCase()+"  -->\r\n").getBytes());
					out.write(("<bean id=\"transDataLoad_"+lineText.substring(2).toLowerCase()+"\" class=\"com.db.trans.load.csv.TransOracleTableDataLoadCsv\">\r\n").getBytes());
					out.write(("	<property name=\"sourceDataSource\" ref=\"sagovSourceDataSource\"/>\r\n").getBytes());
					out.write(("	<property name=\"commitNumber\" value=\"2000\"/>\r\n").getBytes());
					out.write(("	<property name=\"limitTextSize\" value=\"0\"/>\r\n").getBytes());
					out.write(("	<property name=\"tableName\" value=\""+lineText.toLowerCase()+"\"/>\r\n").getBytes());
					out.write(("	<property name=\"loadFileName\" value=\""+lineText.substring(2).toLowerCase()+"\"/>\r\n").getBytes());
					out.write(("	<property name=\"fileNameSuffix\" value=\".dat\"/>\r\n").getBytes());
					out.write(("	<property name=\"terminatedString\" value=\"|+|\"/>\r\n").getBytes());
					out.write(("	<property name=\"listTableColumns\" > \r\n").getBytes());
					out.write(("		<util:list  list-class=\"java.util.ArrayList\">\r\n").getBytes());
			 		
			 		
					
					
					
					_logger.info("-- --No." + sourceCount + " , Table : " + lineText.toUpperCase() + "\r\n");
					//out.write(("--Script Table "+lineText.toUpperCase()+" Start .\r\n").getBytes());
					readTable(lineText);
					out.write(("		</util:list>\r\n").getBytes());
					out.write(("	</property>\r\n").getBytes());
			 		
					out.write(("</bean>\r\n").getBytes());
					out.write(("\r\n").getBytes());
					
					//out.write(("--Script Table "+lineText.toUpperCase()+" End .\r\n\r\n").getBytes());
				}
			}
			bReader.close();
			for(String b :tableList) {
				out.write(("<ref bean=\""+b+"\"/>\r\n").getBytes());
			}
		}else{
			_logger.info("");
		}
		out.flush();
		out.close();
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(String configFilePath) {
		this.configFilePath = configFilePath;
	}

	public String getExportFilePath() {
		return exportFilePath;
	}

	public void setExportFilePath(String exportFilePath) {
		this.exportFilePath = exportFilePath;
	}

	public String getTerminatedString() {
		return terminatedString;
	}

	public void setTerminatedString(String terminatedString) {
		this.terminatedString = terminatedString;
	}

	public String getInFilePath() {
		return inFilePath;
	}

	public void setInFilePath(String inFilePath) {
		this.inFilePath = inFilePath;
	}

	public String getBadFilePath() {
		return badFilePath;
	}

	public void setBadFilePath(String badFilePath) {
		this.badFilePath = badFilePath;
	}

	public String getFileNameSuffix() {
		return fileNameSuffix;
	}

	public void setFileNameSuffix(String fileNameSuffix) {
		this.fileNameSuffix = fileNameSuffix;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}
	
}
