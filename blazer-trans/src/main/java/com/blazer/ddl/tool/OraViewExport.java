/**
 * 
 */
package com.blazer.ddl.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.pipeline.PipeLineTask;


/**
 * @author user
 *
 */
public class OraViewExport  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(OraViewExport.class);
	FileOutputStream out;
	String url;
	String user;
	String pass;
	String driverClass;
	DataSource dataSource;
	String owner="RWUSER";
	String configFilePath;
	String exportFilePath;
	Connection conn;
	
	/**
	 * 
	 */
	public OraViewExport() {
		// TODO Auto-generated constructor stub
	}


	public int  execute() throws Exception{
		//String filePath="E:/MHSHI/ora.txt";
		//String oraExport="E:/MHSHI/oraExport.txt";
		conn = dataSource.getConnection();
		File exportFile=new File(exportFilePath);
		//if(exportFile.exists()){
		//	exportFile.delete();
		//}
		//exportFile.createNewFile();
		
		out=new FileOutputStream(exportFile,true);
		
		readExportData(configFilePath);
		out.flush();
		out.close();
		conn.close();
		return 0;
	}
	public void  readView(String view) throws Exception{
		out.write("\r\n--Create Or Replace View  \r\n".getBytes());
		Statement stmt=conn.createStatement();
		String sql="select AV.OWNER,AV.VIEW_NAME,  AV.text  from sys.All_Views AV WHERE  owner='"+owner+"' and VIEW_NAME='"+view+"'";
		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			//_logger.info("OWNER "+rs.getString("OWNER")+" name :"+rs.getString("name")+" TYPE :"+rs.getString("TYPE")+"\t "+rs.getString("TEXT"));
			//_logger.info(rs.getString("TEXT"));
			out.write(("\r\ncreate or replace view "+view+" as \r\n"+rs.getString("TEXT")+";\r\n").getBytes());
		}
		rs.close();
		stmt.close();
	}
	
	public void  readExportData(String filePath) throws Exception{
		File txtFile=new File(filePath);
		if(txtFile.exists()){
			InputStreamReader read=new InputStreamReader(new FileInputStream(filePath));
			BufferedReader bReader=new BufferedReader(read);
			String lineText;
			int sourceCount=0;
			while((lineText=bReader.readLine())!=null){
				if(lineText.startsWith("#")){
					//
				}else if(lineText.startsWith("--")){
					out.write(("SELECT '"+lineText.substring(2)+"' FROM DUAL;\r\n").getBytes());
				}else if(lineText.startsWith("++")){
					out.write((lineText.substring(2)+"\r\n").getBytes());
				}else{
					_logger.info(lineText);
					sourceCount++;
					out.write(("\r\n-- --No." + sourceCount + " , View : " + lineText + "").getBytes());
					readView(lineText);
				}
			}
			bReader.close();
			read.close();
		}else{
			_logger.info("");
		}
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

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
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

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}


	public DataSource getDataSource() {
		return dataSource;
	}


	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
