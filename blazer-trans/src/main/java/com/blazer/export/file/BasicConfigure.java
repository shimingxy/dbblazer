package com.blazer.export.file;

import javax.sql.DataSource;

public class BasicConfigure {
	protected DataSource sourceDataSource;
	protected String fromUrl;
	protected String fromUser;
	
	protected int  	commitNumber = 2000;
	protected int  	threadSize =1;
	protected int	   limitTextSize=0;
	
	
	protected String fileNameSuffix=".txt";
	protected String terminatedString="|+|";

	protected String exportFilePath;

	protected String tableName;
	protected String outFileName;
	protected String selectFieldsString;
	protected String whereSqlString;
	protected String fileType="csv";
	protected boolean merge=true;
	protected int fileNameUseSysdate = 0;
	
	
	public BasicConfigure() {

	}


	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}


	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
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


	public int getCommitNumber() {
		return commitNumber;
	}


	public void setCommitNumber(int commitNumber) {
		this.commitNumber = commitNumber;
	}


	public int getThreadSize() {
		return threadSize;
	}


	public void setThreadSize(int threadSize) {
		this.threadSize = threadSize;
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


	public String getExportFilePath() {
		return exportFilePath;
	}


	public void setExportFilePath(String exportFilePath) {
		this.exportFilePath = exportFilePath;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getOutFileName() {
		return outFileName;
	}


	public void setOutFileName(String outFileName) {
		this.outFileName = outFileName;
	}


	public String getSelectFieldsString() {
		return selectFieldsString;
	}


	public void setSelectFieldsString(String selectFieldsString) {
		this.selectFieldsString = selectFieldsString;
	}


	public String getWhereSqlString() {
		return whereSqlString;
	}


	public void setWhereSqlString(String whereSqlString) {
		this.whereSqlString = whereSqlString;
	}


	public String getFileType() {
		return fileType;
	}


	public void setFileType(String fileType) {
		this.fileType = fileType;
	}


	public boolean isMerge() {
		return merge;
	}


	public void setMerge(boolean merge) {
		this.merge = merge;
	}


	public int getFileNameUseSysdate() {
		return fileNameUseSysdate;
	}


	public void setFileNameUseSysdate(int fileNameUseSysdate) {
		this.fileNameUseSysdate = fileNameUseSysdate;
	}
	
	
	
}
