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
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.pipeline.PipeLineTask;


/**
 * @author user
 *
 */
public class OraTableGreenplumI2D  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(OraTableGreenplumI2D.class);
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
	String fromTablePrefix;
	String toTablePrefix;
	
	/**
	 * 
	 */
	public OraTableGreenplumI2D() {
		// TODO Auto-generated constructor stub
	}

	
	public int   execute() throws Exception{
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
		out.write(("DELETE FROM "+(toTablePrefix+tableName).toUpperCase()+" WHERE ETL_FLAG='${_ETL_FLAG}' ;\r\n").getBytes());
		out.write(("COMMIT;\r\n").getBytes());
		//tableName=(tablePrefix+tableName);
		out.write(("INSERT INTO "+(toTablePrefix+tableName).toUpperCase()).getBytes());
		out.write("(\r\n".getBytes());
		for (int i=0;i<tcArray.size();i++){
			TableColumns tc=tcArray.get(i);
			out.write(("\t"+String.format("%-15s", tc.getColumnName())).getBytes());
			if(i<tcArray.size()-1){
				out.write(",\r\n".getBytes());
			}else{
				out.write("\r\n".getBytes());
			}
		}
		//comment on column ALARMAPPEVALUATE.signalgrade1 is '�źŽ���ȼ�';
		//comment on table ALARMAPPEVALUATE  is 'Ԥ���ź�Ӧ�����۱�';
		out.write((")\r\n").getBytes());
		out.write(("SELECT  \r\n").getBytes());
		for (int i=0;i<tcArray.size();i++){
			TableColumns tc=tcArray.get(i);
			out.write(("\t"+String.format("%-15s", tc.getColumnName())).getBytes());
			if(i<tcArray.size()-1){
				out.write(",\r\n".getBytes());
			}else{
				out.write("\r\n".getBytes());
			}
		}
		out.write(("FROM  "+(fromTablePrefix+tableName).toUpperCase()+";\r\n").getBytes());
		out.write(("COMMIT;\r\n").getBytes());
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
			//StringBuffer sb_dropTable=new StringBuffer();
			while((lineText=bReader.readLine())!=null){
				if(lineText.startsWith("#")){
					//���?
				}else if(lineText.startsWith("--")){
					out.write(("SELECT '"+lineText.substring(2)+"' FROM DUAL;\r\n").getBytes());
				}else if(lineText.startsWith("++")){
					out.write((lineText.substring(2)+"\r\n").getBytes());
				}else{
					_logger.info(lineText);
					sourceCount++;
					//out.write(("-- Greenplum �������ṹ����\r\n").getBytes());
					out.write(("-- --No." + sourceCount + " , Table : " + lineText.toUpperCase() + "\r\n").getBytes());
					//out.write(("--Script Table "+lineText.toUpperCase()+" Start .\r\n").getBytes());
					//readTable(lineText,"dwmart_rwms.");
					readTable(lineText);
					readTableCons(lineText);
					//sb_dropTable.append("DROP TABLE ").append(this.tablePrefix).append(lineText.toUpperCase()).append(";\r\n");
					//out.write(("-- Greenplum ��ʱ���ṹ����\r\n").getBytes());
					//readTable(lineText,"dwdata_sdata.RWM_I_");
					//readTableIndex(lineText);
					//readTableGrant(lineText);
					//out.write(("--Script Table "+lineText.toUpperCase()+" End .\r\n\r\n").getBytes());
				}
			}
			//out.write(("-- --DROP TABLE\r\n").getBytes());
			//out.write((sb_dropTable.toString()).getBytes());
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


	public String getFromTablePrefix() {
		return fromTablePrefix;
	}


	public void setFromTablePrefix(String fromTablePrefix) {
		this.fromTablePrefix = fromTablePrefix;
	}


	public String getToTablePrefix() {
		return toTablePrefix;
	}


	public void setToTablePrefix(String toTablePrefix) {
		this.toTablePrefix = toTablePrefix;
	}



}
