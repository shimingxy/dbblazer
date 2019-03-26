/**
 * 
 */
package com.blazer.ddl;

import java.io.FileOutputStream;
import java.sql.Connection;
import javax.sql.DataSource;


/**
 * @author user
 *
 */
public class DDLExportBase {
	protected DataSource dataSource;
	protected Connection conn;
	protected FileOutputStream out;
	protected String url;
	protected String user;
	protected String pass;
	protected String driverClass;
	
	protected String owner;
	
	protected String configFilePath;
	protected String exportFilePath;
	protected String tablePrefix;
	protected String grantUser;
	protected String permission;
	protected String engine ;
	protected String toDbType;
	
	
	/**
	 * 
	 */
	public DDLExportBase() {

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
