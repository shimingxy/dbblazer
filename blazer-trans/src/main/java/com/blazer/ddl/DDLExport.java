/**
 * 
 */
package com.blazer.ddl;

import java.io.FileOutputStream;
import java.sql.Connection;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.pipeline.PipeLineTask;


/**
 * @author user
 *
 */
public class DDLExport  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(DDLExport.class);
	
	DataSource dataSource;
	Connection conn;
	FileOutputStream out;
	String url;
	String user;
	String pass;
	String driverClass;
	
	String owner;
	
	String configFilePath;
	String exportFilePath;
	String tablePrefix;
	String grantUser;
	String permission;
	String engine ;
	String toDbType;
	
	
	
	
	/**
	 * 
	 */
	public DDLExport() {
		// TODO Auto-generated constructor stub
	}

	
	public int  execute() throws Exception{
		
		_logger.debug(dataSource.toString());
		_logger.debug("DLL TO "+toDbType);
		
		if(dataSource.toString().toLowerCase().indexOf("oracle")>-1) {
			if(toDbType.equalsIgnoreCase("oracle")) {
				DDLOracle2Oracle dll=new DDLOracle2Oracle();
				dll.setDataSource(dataSource);
				dll.setConfigFilePath(configFilePath);
				dll.setExportFilePath(exportFilePath);
				dll.setOwner(owner);
				dll.execute();
				
			}else if(toDbType.equalsIgnoreCase("mysql")) {
				DDLOracle2Mysql dll=new DDLOracle2Mysql();
				dll.setDataSource(dataSource);
				dll.setExportFilePath(exportFilePath);
				dll.setEngine(engine);
				dll.execute();
			}else if(toDbType.equalsIgnoreCase("greenplum")) {
				DLLOracle2Greenplum dll=new DLLOracle2Greenplum();
				dll.setDataSource(dataSource);
				dll.setConfigFilePath(configFilePath);
				dll.setExportFilePath(exportFilePath);
				dll.setOwner(owner);
				dll.setGrantUser(grantUser);
				dll.setTablePrefix(tablePrefix);
				dll.execute();
			}else if(toDbType.equalsIgnoreCase("db2")) {
				
			}
		}else if(dataSource.toString().toLowerCase().indexOf("mysql")>-1) {

		}else if(dataSource.toString().toLowerCase().indexOf("greenplum")>-1) {

		}else if(dataSource.toString().toLowerCase().indexOf("db2")>-1) {
			if(toDbType.equalsIgnoreCase("oracle")) {
				DDLDB22Oracle dll=new DDLDB22Oracle();
				dll.setDataSource(dataSource);
				dll.setExportFilePath(configFilePath);
				dll.setOwner(owner);
				dll.execute();
				
			}
		}
		return 0;
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


	public String getTablePrefix() {
		return tablePrefix;
	}


	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}


	public String getGrantUser() {
		return grantUser;
	}


	public void setGrantUser(String grantUser) {
		this.grantUser = grantUser;
	}


	public String getPermission() {
		return permission;
	}


	public void setPermission(String permission) {
		this.permission = permission;
	}


	public String getEngine() {
		return engine;
	}


	public void setEngine(String engine) {
		this.engine = engine;
	}


	public String getToDbType() {
		return toDbType;
	}


	public void setToDbType(String toDbType) {
		this.toDbType = toDbType;
	}

}
