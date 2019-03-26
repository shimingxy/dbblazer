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
public class OraSourceExport  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(OraSourceExport.class);
	FileOutputStream out;
	DataSource dataSource;
	String url;
	String user;
	String pass;
	String driverClass;
	
	String configFilePath;
	String exportFilePath;
	Connection conn;
	
	/**
	 * 
	 */
	public OraSourceExport() {
		// TODO Auto-generated constructor stub
	}

	public int  execute() throws Exception{
		conn = dataSource.getConnection();
		_logger.info("---exportFilePath "+exportFilePath);
		File exportFile=new File(exportFilePath);
		if(exportFile.exists()){
			exportFile.delete();
		}
		exportFile.createNewFile();
		out=new FileOutputStream(exportFile);
		out.write(("-- --FROM SERVER " + this.url + "\n").getBytes());
		out.write(("-- --FROM USER " + this.user + "\n").getBytes());
		out.write(("-- --TO FILE " + this.exportFilePath + "\n").getBytes());
		readExportData(configFilePath);
		out.flush();
		out.close();
		conn.close();
		return 0;
	}
	public void  readSource(String owner,String name,String type) throws Exception{
		Statement stmt=conn.createStatement();
		String sql="select DISTINCT SC.OWNER,SC.name,SC.TYPE,SC.line,SC.TEXT  from sys.All_Source SC WHERE  owner='"+owner+"' and name='"+name+"' and  type='"+type+"' order by SC.OWNER,SC.name,SC.line";
		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			//_logger.info("OWNER "+rs.getString("OWNER")+" name :"+rs.getString("name")+" TYPE :"+rs.getString("TYPE")+"\t "+rs.getString("TEXT"));
			//_logger.info(rs.getString("TEXT"));
			out.write(rs.getString("TEXT").getBytes());
		}
		out.write("/\r\n".getBytes());
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
					out.write(("-- --No." + sourceCount + " , Source : " + lineText + "\n").getBytes());
					String []param=lineText.split(",");
					readSource(param[0],param[1],param[2]);
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

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
