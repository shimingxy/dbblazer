/**
 * 
 */
package com.blazer.ddl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;


/**
 * @author user
 *
 */
public class DLLOracle2Greenplum  extends DDLExportBase{
	private static final Logger _logger = LoggerFactory.getLogger(DLLOracle2Greenplum.class);
	
	/**
	 * 
	 */
	public DLLOracle2Greenplum() {
		// TODO Auto-generated constructor stub
	}

	
	public int  execute() throws Exception{
		//String filePath="E:/MHSHI/ora.txt";
		//String oraExport="E:/MHSHI/oraExport.txt";
		conn = dataSource.getConnection();
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
	
	public void  readTable(String tableName) throws Exception{
		out.write("--Create Table \r\n".getBytes());
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
		String TABLECOMMENTS=null;
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
		_logger.info("tcArray : "+tcArray);
		tableName=(tablePrefix+tableName);
		out.write(("create table "+tableName.toUpperCase()).getBytes());
		out.write("(\r\n".getBytes());
		String dk=null;
		for (int i=0;i<tcArray.size();i++){
			TableColumns tc=tcArray.get(i);
			if(dk==null)dk=tc.getColumnName();
			TABLECOMMENTS=tc.getTableComments();
			int defaultType=0;
			out.write((" \t"+String.format("%-20s", tc.getColumnName()) +"\t").getBytes());
			if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
					tc.getDataType().equalsIgnoreCase("VARCHAR")||
					tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
					tc.getDataType().equalsIgnoreCase("CHAR")||
					tc.getDataType().equalsIgnoreCase("RAW")
					){
				defaultType=1;
				out.write(("character varying("+tc.getDataLength()+")").getBytes());
				
			}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
				defaultType=2;
				if(tc.getDataLength()==22&&tc.getDataScale()>0){
					out.write(("NUMERIC").getBytes());
				}else if(tc.getDataLength()==22&&tc.getDataScale()==0){
					out.write(("NUMERIC").getBytes());
				}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){
					out.write(("NUMERIC("+tc.getDataPrecision()+")").getBytes());
				}else{//Double
					out.write(("(NUMERIC"+tc.getDataPrecision()+","+tc.getDataScale()+")").getBytes());
				}
			}else if(tc.getDataType().equalsIgnoreCase("FLOAT")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("blob")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("clob")){
				out.write(("text").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("DATE")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("NCLOB")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("LONG")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("ROWID")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("BINARY_DOUBLE")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().equalsIgnoreCase("BINARY_FLOAT")){
				out.write((tc.getDataType()+"").getBytes());
			}else if(tc.getDataType().indexOf(")")>-1){
				out.write((tc.getDataType()+"").getBytes());
			}else {
				out.write((tc.getDataType()+"").getBytes());
			}
			
			if(defaultType>0&&tc.getDefaultValue()!=null&&!tc.getDefaultValue().equals("")){
				if(defaultType==1){
					out.write((" default '"+tc.getDefaultValue().replace("\n", "").replace("\r", "")+"'").getBytes());
				}else if(defaultType==2){
					out.write((" default "+tc.getDefaultValue().replace("\n", "").replace("\r", "")+"").getBytes());
				}
			}
			
			if(!tc.getNullAble().equalsIgnoreCase("Y")){
				out.write(" NOT NULL ".getBytes());
			}
			
			if(i<tcArray.size()-1){
				out.write(",\r\n".getBytes());
			}else{
				out.write("\r\n".getBytes());
			}
		}
		//comment on column ALARMAPPEVALUATE.signalgrade1 is '�źŽ���ȼ�';
		//comment on table ALARMAPPEVALUATE  is 'Ԥ���ź�Ӧ�����۱�';
		out.write((")DISTRIBUTED BY ("+dk+");\r\n\r\n").getBytes());
		
		if(TABLECOMMENTS!=null&&!TABLECOMMENTS.equals("")){
			out.write(("COMMENT ON TABLE  "+String.format("%-40s", tableName.toUpperCase())+" IS '"+TABLECOMMENTS.trim()+"';\r\n\r\n").getBytes());
		}
		for (int i=0;i<tcArray.size();i++){
			TableColumns tc=tcArray.get(i);
			if(tc.getColumnComments()!=null&&!tc.getColumnComments().equals("")){
				out.write(("COMMENT ON COLUMN "+String.format("%-40s", tableName.toUpperCase()+"."+tc.getColumnName())+" IS '"+tc.getColumnComments().trim()+"';\r\n").getBytes());
			}
		}
		out.write("\r\n".getBytes());
		if(grantUser!=null && !grantUser.equals("")){
			out.write(("GRANT "+this.permission+" ON "+tableName.toUpperCase()+" TO "+grantUser+";\r\n").getBytes());
		}
		
		rs.close();
		stmt.close();
		
	}
	
	public void  readTableCons(String tableName) throws Exception{
		out.write("--Create Table Constraints \r\n".getBytes());
		Statement stmt=conn.createStatement();
		/*String sql="select t.owner,t.table_name,c.COMMENTS from sys.all_all_tables t,sys.all_tab_comments c "+
					" where t.table_name=c.TABLE_NAME "+
					" and t.table_name='"+tableName+"' and t.owner ='"+owner+"' "+
					" order by t.owner,t.table_name";*/
		String sql="select ac.OWNER,ac.CONSTRAINT_NAME,ac.CONSTRAINT_TYPE,ac.TABLE_NAME,acc.COLUMN_NAME,acc.POSITION " +
				" from sys.all_constraints ac ,sys.all_cons_columns acc "  +
				" where ac.OWNER=acc.OWNER  "  +
				" and ac.CONSTRAINT_NAME=acc.CONSTRAINT_NAME "  +
				" and ac.TABLE_NAME='"+tableName.toUpperCase()+"' "  +
				" and ac.owner ='"+owner+"'"+
				" and acc.POSITION IS NOT NULL "+
				" order by ac.CONSTRAINT_NAME, acc.POSITION";
		
		_logger.info(sql);
		HashMap<String,ArrayList<String>> propMap=new HashMap<String,ArrayList<String>>();
		HashMap<String,String> keyTypeMap=new HashMap<String,String>();
		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			_logger.info("CONSTRAINT_NAME : "+rs.getString("CONSTRAINT_NAME"));
			if(!propMap.containsKey(rs.getString("CONSTRAINT_NAME"))){
				propMap.put(rs.getString("CONSTRAINT_NAME"),new ArrayList<String>());
			}
			keyTypeMap.put(rs.getString("CONSTRAINT_NAME"), rs.getString("CONSTRAINT_TYPE"));
			propMap.get(rs.getString("CONSTRAINT_NAME")).add(rs.getString("COLUMN_NAME"));
		}
		
		for (String key :propMap.keySet()){
			String indStr="alter table "+owner+"."+tableName+" add constraint "+key;
			if(!keyTypeMap.containsKey(key))continue;
			//primary, unique and foreign
			if(keyTypeMap.get(key).equalsIgnoreCase("U")){
				indStr=indStr+" unique key ";
			}
			if(keyTypeMap.get(key).equalsIgnoreCase("P")){
				indStr=indStr+" primary key ";
			}
			indStr=indStr+"(";
			int i=0;
			for(String c :propMap.get(key)){
				if(i++==0){
					indStr=indStr+""+c;
				}else{
					indStr=indStr+","+c;
				}
			}
			indStr=indStr+");\r\n";
			out.write(indStr.getBytes());
			
		}
		out.write("\r\n".getBytes());
		rs.close();
		stmt.close();
	}
		
	public void  readTableIndex(String tableName) throws Exception{
		out.write("--Create Table Indexs \r\n".getBytes());
		Statement stmt=conn.createStatement();
		String sql="select ai.OWNER,ai.INDEX_NAME,aic.COLUMN_NAME,aic.COLUMN_POSITION " +
				" from sys.all_indexes ai,sys.all_ind_columns aic "+
				" where ai.OWNER=aic.INDEX_OWNER "+
				" and ai.INDEX_NAME=aic.INDEX_NAME "+
				" and ai.TABLE_NAME=aic.TABLE_NAME "+
				" and ai.TABLE_NAME='"+tableName.toUpperCase()+"' "+
				" and ai.owner ='"+owner+"'"+
				" order by ai.INDEX_NAME,aic.COLUMN_POSITION ";
		
		_logger.info(sql);
		HashMap<String,ArrayList<String>> propMap=new HashMap<String,ArrayList<String>>();
		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			_logger.info("INDEX_NAME : "+rs.getString("INDEX_NAME"));
			if(!propMap.containsKey(rs.getString("INDEX_NAME"))){
				propMap.put(rs.getString("INDEX_NAME"),new ArrayList<String>());
			}
			propMap.get(rs.getString("INDEX_NAME")).add(rs.getString("COLUMN_NAME"));
		}
		
		for (String key :propMap.keySet()){
			String indStr="create index "+owner+"."+key+" on "+owner+"."+tableName;
			indStr=indStr+"(";
			int i=0;
			for(String c :propMap.get(key)){
				if(i++==0){
					indStr=indStr+""+c;
				}else{
					indStr=indStr+","+c;
				}
			}
			indStr=indStr+");\r\n";
			out.write(indStr.getBytes());
			out.write("\r\n".getBytes());
		}
		
		rs.close();
		stmt.close();
		
	}	
	
	public void  readTableGrant(String tableName) throws Exception{
		out.write("--Grant Privilege \r\n".getBytes());
		Statement stmt=conn.createStatement();
		String sql="select atp.GRANTOR,atp.GRANTEE,atp.TABLE_SCHEMA,atp.TABLE_NAME,atp.privilege,atp.GRANTABLE " +
				" from sys.all_tab_privs atp  where  TABLE_NAME='"+tableName.toUpperCase()+"'";
		
		_logger.info(sql);
		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			out.write(("GRANT "+String.format("%-10s", rs.getString("privilege")) +" ON "+rs.getString("TABLE_SCHEMA")+"."+tableName.toUpperCase()+" to "+rs.getString("GRANTEE")+";\r\n").getBytes());
			//grant select, insert, update, delete, references, alter, index on RWMSUSER.CONTROLTHING to RWUSER;
		}
		out.write("\r\n".getBytes());
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
			ArrayList<String> tableArray=new ArrayList<String>();
			StringBuffer sb_dropTable=new StringBuffer();
			while((lineText=bReader.readLine())!=null){
				tableArray.add(lineText);
				if(lineText.startsWith("#")){
					//���?
				}else if(lineText.startsWith("--")){
					//out.write(("SELECT '"+lineText.substring(2)+"' FROM DUAL;\r\n").getBytes());
				}else if(lineText.startsWith("++")){
					//out.write((lineText.substring(2)+"\r\n").getBytes());
				}else{
					_logger.info(lineText);
					sourceCount++;
					sb_dropTable.append("DROP TABLE IF EXISTS ").append(this.tablePrefix).append(lineText.toUpperCase()).append(";\r\n");
					//out.write(("-- Greenplum ��ʱ���ṹ����\r\n").getBytes());
					//readTable(lineText,"dwdata_sdata.RWM_I_");
					//readTableIndex(lineText);
					//readTableGrant(lineText);
					//out.write(("--Script Table "+lineText.toUpperCase()+" End .\r\n\r\n").getBytes());
				}
			}
			
			
			out.write(("-- --DROP TABLE\r\n").getBytes());
			out.write((sb_dropTable.toString()).getBytes());
			
			sourceCount=0;
			for(String table :tableArray){
				if(table.startsWith("#")){
					//���?
				}else if(table.startsWith("--")){
					out.write(("SELECT '"+table.substring(2)+"' FROM DUAL;\r\n").getBytes());
				}else if(table.startsWith("++")){
					out.write((table.substring(2)+"\r\n").getBytes());
				}else{
					_logger.info(table);
					sourceCount++;
					//out.write(("-- Greenplum �������ṹ����\r\n").getBytes());
					out.write(("-- --No." + sourceCount + " , Table : " + table.toUpperCase() + "\r\n").getBytes());
					//out.write(("--Script Table "+lineText.toUpperCase()+" Start .\r\n").getBytes());
					//readTable(lineText,"dwmart_rwms.");
					readTable(table);
					//readTableCons(table);
					//sb_dropTable.append("DROP TABLE ").append(this.tablePrefix).append(lineText.toUpperCase()).append(";\r\n");
					//out.write(("-- Greenplum ��ʱ���ṹ����\r\n").getBytes());
					//readTable(lineText,"dwdata_sdata.RWM_I_");
					//readTableIndex(lineText);
					//readTableGrant(lineText);
					//out.write(("--Script Table "+lineText.toUpperCase()+" End .\r\n\r\n").getBytes());
				}
			}
			bReader.close();
			read.close();
		}else{
			_logger.info("");
		}
	}

}
