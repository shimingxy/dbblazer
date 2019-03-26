/**
 * 
 */
package com.blazer.db;

import java.util.ArrayList;

/**
 * @author user
 *
 */
public class TableDescribe {
	String owner;
	String tableName;
	String tableComments;
	
	//字段列表
	ArrayList<TableColumns> 	tableColumnsList=new ArrayList<TableColumns>();
	//主键 约束等
	TableConstraints constraints=new TableConstraints();
	
	//for trans database
	String targetTableName;
	//for trans database where
	String whereSqlString="";
	
	public TableDescribe(){}


	public TableDescribe(String owner, String tableName, String tableComments) {
		super();
		this.owner = owner;
		this.tableName = tableName;
		this.tableComments = tableComments;
	}


	public String getOwner() {
		return owner;
	}


	public void setOwner(String owner) {
		this.owner = owner;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getTableComments() {
		return tableComments;
	}


	public void setTableComments(String tableComments) {
		this.tableComments = tableComments;
	}


	public ArrayList<TableColumns> getTableColumnsList() {
		return tableColumnsList;
	}


	public void setTableColumnsList(ArrayList<TableColumns> tableColumnsList) {
		this.tableColumnsList = tableColumnsList;
	}


	public TableConstraints getConstraints() {
		return constraints;
	}


	public void setConstraints(TableConstraints constraints) {
		this.constraints = constraints;
	}


	public String getTargetTableName() {
		return targetTableName;
	}


	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}


	public String getWhereSqlString() {
		return whereSqlString;
	}


	public void setWhereSqlString(String whereSqlString) {
		this.whereSqlString = whereSqlString;
	}


	@Override
	public String toString() {
		return "TableDescribe [owner=" + owner + ", tableName=" + tableName + ", tableComments=" + tableComments
				+ ", tableColumnsList=" + tableColumnsList + ", constraints=" + constraints + ", targetTableName="
				+ targetTableName + ", whereSqlString=" + whereSqlString + "]";
	}
}
