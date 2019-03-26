package com.blazer.db;

import java.util.ArrayList;
import java.util.HashMap;

public class TableConstraints {
	
	HashMap<String,ArrayList<String>> constraintMap=new HashMap<String,ArrayList<String>>();
	//约束的类型 primary 主键 ，unique 唯一建,index 索引
	HashMap<String,String> 	constraintTypeMap=new HashMap<String,String>();
	
	public TableConstraints() {
		super();
	}
	
	public void setConstraintType(String constraint,String type) {
		constraintTypeMap.put(constraint, type);
	}
	public String getConstraintType(String constraint) {
		return constraintTypeMap.get(constraint);
	}
	
	public void setConstraint(String constraint,ArrayList<String> columns) {
		constraintMap.put(constraint, columns);
	}
	
	public void addConstraint(String constraint,String column) {
		if(!constraintMap.containsKey(constraint)) {
			constraintMap.put(constraint,new ArrayList<String>());
		}
		constraintMap.get(constraint).add(column);
	}
	
	public ArrayList<String> getConstraint(String constraint) {
		return constraintMap.get(constraint);
	}

	public HashMap<String, ArrayList<String>> getConstraintMap() {
		return constraintMap;
	}

	public HashMap<String, String> getConstraintTypeMap() {
		return constraintTypeMap;
	}

	public void setConstraintTypeMap(HashMap<String, String> constraintTypeMap) {
		this.constraintTypeMap = constraintTypeMap;
	}

	public void setConstraintMap(HashMap<String, ArrayList<String>> constraintMap) {
		this.constraintMap = constraintMap;
	}
	
}
