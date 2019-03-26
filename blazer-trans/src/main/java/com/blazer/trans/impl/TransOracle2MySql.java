/**
 * 
 */
package com.blazer.trans.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.db.TableDescribe;


/**
 * @author mhshi
 * 
 */
public class TransOracle2MySql extends TransBase{
	private static final Logger _logger = LoggerFactory.getLogger(TransOracle2MySql.class);
	
	public TransOracle2MySql(
			DataSource sourceDataSource,
			DataSource targetDataSource,
			ArrayList<TableDescribe> listTables, 
			String transType,
			String owner,
			int commitNum,
			int threadNum) {
		super();
		this.sourceDataSource = sourceDataSource;
		this.targetDataSource = targetDataSource;
		this.commitNum = commitNum;
		this.listTables = listTables;
		this.threadNum=threadNum;
		this.owner=owner;
		this.transType=transType;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		for(TableDescribe tableQuery :listTables){
			listTableColumns = new ArrayList<TableColumns>();
			listTablePKs = new ArrayList<String>();
			listTableLobColumns = new ArrayList<TableColumns>();
			table=tableQuery;
			try {
				getRecordCount(tableQuery.getTableName()+tableQuery.getWhereSqlString());
				if(table.getTargetTableName()==null ||table.getTargetTableName().equalsIgnoreCase("")) {
					table.setTargetTableName(table.getTableName());
				}
				_logger.info("--thread "+threadNum+" --Trans Record Count "+recordsCount+" , Table " + tableQuery.getTableName().toUpperCase());
				if(recordsCount<=0)continue;
				
				Connection sourcConn=sourceDataSource.getConnection();
				Connection targetConn=targetDataSource.getConnection();
				Statement  targetStmt=targetConn.createStatement();
				if(transType.equalsIgnoreCase("FULL")){
					deleteRecordsCount=targetStmt.executeUpdate("TRUNCATE TABLE "+tableQuery.getTargetTableName()+tableQuery.getWhereSqlString());
					_logger.info("--thread "+threadNum+" --Trans Delete Records "+deleteRecordsCount+" , TRUNCATE TABLE "+tableQuery.getTargetTableName()+tableQuery.getWhereSqlString());
				}else if(transType.equalsIgnoreCase("INCREMENT")){
					deleteRecordsCount=targetStmt.executeUpdate("DELETE FROM "+tableQuery.getTargetTableName()+tableQuery.getWhereSqlString());
					_logger.info("--thread "+threadNum+" --Trans Delete Records "+deleteRecordsCount+" , DELETE FROM "+tableQuery.getTargetTableName()+tableQuery.getWhereSqlString());
				}
				targetStmt.close();
				targetConn.close();
				
				Statement  sourcStmt = sourcConn.createStatement();
				ResultSet  sourceRs=sourcStmt.executeQuery("SELECT * FROM "+tableQuery.getTableName()+tableQuery.getWhereSqlString());
				buildMetaData(sourceRs);
				getTableCons();
				buildInsertSql();
				batchInsert(sourceRs);
				sourceRs.close();
				sourcStmt.close();
				sourcConn.close();
				Thread.sleep(2000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		_logger.info("--thread "+threadNum+" -- Complete .");
	}
	
	public void batchInsert(ResultSet rs) throws Exception{
		Connection targetConn=targetDataSource.getConnection();
		targetConn.setAutoCommit(false);
		PreparedStatement targetPstmt = targetConn.prepareStatement(insertSql);
		long insertNum=0;
		long commitCount=0;
		while(rs.next()){
			int pos=1;
			for(TableColumns tc : listTableColumns){
				//_logger.info("--column "+tc.getColumnName()+" , "+tc.getDataType() );
				if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
						tc.getDataType().equalsIgnoreCase("VARCHAR")||
						tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
						tc.getDataType().equalsIgnoreCase("CHAR")||
						tc.getDataType().equalsIgnoreCase("RAW")
						){
					//_logger.info("==="+tc.getColumnName());
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
					if(tc.getDataLength()==22&&tc.getDataScale()>0){//NUMBER
						targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else if(tc.getDataLength()==22&&tc.getDataScale()==0){//INTEGER
						targetPstmt.setInt(pos, rs.getInt(tc.getColumnName()));
					}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){//LONG
						targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else{//DOUBLE
						targetPstmt.setDouble(pos, rs.getDouble(tc.getColumnName()));
					}
				}else if(tc.getDataType().equalsIgnoreCase("blob")){
					
				}else if(tc.getDataType().equalsIgnoreCase("clob")){
					
				}else if(tc.getDataType().equalsIgnoreCase("DATE")){
					targetPstmt.setDate(pos, rs.getDate(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("NCLOB")){
					//targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("LONG")){
					targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("ROWID")){
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
					//targetPstmt.setRowId(pos, rs.getRowId(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("BINARY_DOUBLE")){
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("BINARY_FLOAT")){
					targetPstmt.setFloat(pos, rs.getFloat(tc.getColumnName()));
				}else if(tc.getDataType().indexOf(")")>-1){
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
				}else{
					targetPstmt.setObject(pos, rs.getObject(tc.getColumnName()));
				}
				
				if(tc.getDataType().equalsIgnoreCase("BLOB")||tc.getDataType().equalsIgnoreCase("CLOB")||tc.getDataType().equalsIgnoreCase("NCLOB")){
					
				}else{
					pos++;
				}
			}
			
			commitCount++;
			if(hasLob){
				targetPstmt.execute();
				targetConn.commit();
				updateLob(rs);
				_logger.info("--thread "+threadNum+"--Commit Has LOB Count "+commitCount );
			}else{
				targetPstmt.addBatch();
				insertNum += 1;
				if (insertNum >= this.commitNum) {
					insertNum = 0;
					targetPstmt.executeBatch();
					targetConn.commit();
					_logger.info("--thread "+threadNum+"--Commit Count "+commitCount );
				}
			}
		}
		if (insertNum >0){
			targetPstmt.executeBatch();
			targetConn.commit();
			_logger.info("--thread "+threadNum+"--Commit Count "+commitCount +" Complete .");
		}
		
		_logger.info("--thread "+threadNum+"--Commit Count "+commitCount +" Complete .");
		targetPstmt.close();
		targetConn.close();
	}
	
	public void updateLob(ResultSet rs) throws Exception{
		Connection targetConn=targetDataSource.getConnection();
		PreparedStatement targetPstmt=targetConn.prepareStatement(updateLobSql);
		
		int pos=1;
		for(String pk :listTablePKs){
			for(TableColumns tc : listTableColumns){
				if(!pk.equalsIgnoreCase(tc.getColumnName()))continue;
				//_logger.info("--column "+tc.getColumnName()+" , "+tc.getDataType() );
				if(tc.getDataType().equalsIgnoreCase("VARCHAR2")||
						tc.getDataType().equalsIgnoreCase("VARCHAR")||
						tc.getDataType().equalsIgnoreCase("NVARCHAR2")||
						tc.getDataType().equalsIgnoreCase("CHAR")||
						tc.getDataType().equalsIgnoreCase("RAW")
						){
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("NUMBER")){
					if(tc.getDataLength()==22&&tc.getDataScale()>0){//NUMBER
						targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else if(tc.getDataLength()==22&&tc.getDataScale()==0){//INTEGER
						targetPstmt.setInt(pos, rs.getInt(tc.getColumnName()));
					}else if(tc.getDataPrecision()==0||tc.getDataScale()==0||tc.getDataScale()==0){//LONG
						targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
					}else{//DOUBLE
						targetPstmt.setDouble(pos, rs.getDouble(tc.getColumnName()));
					}
				}else if(tc.getDataType().equalsIgnoreCase("blob")){
					
				}else if(tc.getDataType().equalsIgnoreCase("clob")){
					
				}else if(tc.getDataType().equalsIgnoreCase("DATE")){
					targetPstmt.setDate(pos, rs.getDate(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("LONG")){
					targetPstmt.setLong(pos, rs.getLong(tc.getColumnName()));
				}else if(tc.getDataType().equalsIgnoreCase("ROWID")){
					targetPstmt.setString(pos, rs.getString(tc.getColumnName()));
					//targetPstmt.setRowId(pos, rs.getRowId(tc.getColumnName()));
				}else{
					targetPstmt.setObject(pos, rs.getObject(tc.getColumnName()));
				}
				pos++;
			}
		}
		
		ResultSet targetRs=targetPstmt.executeQuery();
		if(targetRs.next()){
			for(TableColumns tc:listTableLobColumns){
				if(tc.getDataType().equalsIgnoreCase("BLOB")){
					updateBlob(rs,targetRs,tc.getColumnName());
				}else if(tc.getDataType().equalsIgnoreCase("CLOB")){
					updateClob(rs,targetRs,tc.getColumnName());
				}else if(tc.getDataType().equalsIgnoreCase("NCLOB")){
					updateClob(rs,targetRs,tc.getColumnName());
				} 
			}
			targetConn.commit();
			targetConn.close();
		}
	}
	
	@SuppressWarnings("deprecation")
	public void updateClob(ResultSet sourceRs,ResultSet targetRs,String column) throws Exception{
		oracle.sql.CLOB sb=(oracle.sql.CLOB)sourceRs.getClob(column);
		if(sb==null)return;
		Reader is=sb.getCharacterStream();
		char[]data=new char[(int)sb.length()];
		is.read(data);
		is.close();
		
		oracle.sql.CLOB tb=(oracle.sql.CLOB)targetRs.getClob(column);
		Writer os=tb.getCharacterOutputStream();
		os.write(data,0,data.length);
		os.flush();
		os.close();
	}
	
	@SuppressWarnings("deprecation")
	public void updateBlob(ResultSet sourceRs,ResultSet targetRs,String column) throws Exception{
		oracle.sql.BLOB sb=(oracle.sql.BLOB)sourceRs.getBlob(column);
		if(sb==null)return;
		InputStream is=sb.getBinaryStream();
		byte[]data=new byte[(int)sb.length()];
		is.read(data);
		is.close();
		
		oracle.sql.BLOB tb=(oracle.sql.BLOB)targetRs.getBlob(column);
		OutputStream os=tb.getBinaryOutputStream();
		os.write(data,0,data.length);
		os.flush();
		os.close();
		
	}
	public void buildMetaData(ResultSet rs) throws SQLException{
		ResultSetMetaData metaData = rs.getMetaData();
		_logger.info("--thread "+threadNum+"--column Count "+metaData.getColumnCount() );
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			TableColumns tc=new TableColumns();
			tc.setColumnName(metaData.getColumnName(i));
			tc.setDataType(metaData.getColumnTypeName(i));
			tc.setTableName(metaData.getTableName(i));
			tc.setDataPrecision(metaData.getPrecision(i));
			tc.setDataScale(metaData.getScale(i));
			_logger.info("--thread "+threadNum+"--No. "+i+" , Column "+tc.getColumnName()+" , DataType "+tc.getDataType() );
			listTableColumns.add(tc);
		}
	}
	
	public void getRecordCount(String tableQuery) throws SQLException{
        String countSql="SELECT COUNT(*) COUNTROWS  FROM "+tableQuery;
        _logger.debug("--thread "+threadNum+" countSql : "+countSql);
        Connection conn=sourceDataSource.getConnection();
		Statement  sourcStmt = conn.createStatement();
        ResultSet  contResultSet = sourcStmt.executeQuery(countSql);
        insertRecordsCount=0;
        recordsCount = 0;
        while(contResultSet.next()){
        	recordsCount = contResultSet.getLong(1); 
        }
        _logger.debug("--thread "+threadNum+" recordsCount : "+recordsCount);
	}
	public void buildInsertSql(){
		insertSql="INSERT INTO `"+table.getTargetTableName()+"`";
		updateLobSql="SELECT  ";
		String c_str="";
		String v_str="";
		String where_sql="     ";
		for (TableColumns tc : listTableColumns){
			c_str=c_str+"`"+tc.getColumnName()+"` ,";
			if(tc.getDataType().equalsIgnoreCase("BLOB")){
				hasLob=true;
				v_str=v_str+" empty_blob() ,";
				updateLobSql=updateLobSql+" `"+tc.getColumnName()+"` ,";
				listTableLobColumns.add(tc);
			}else if(tc.getDataType().equalsIgnoreCase("CLOB")){
				hasLob=true;
				v_str=v_str+" empty_clob() ,";
				updateLobSql=updateLobSql+" `"+tc.getColumnName()+"` ,";
				listTableLobColumns.add(tc);
			}else if(tc.getDataType().equalsIgnoreCase("NCLOB")){
				hasLob=true;
				v_str=v_str+" empty_clob() ,";
				updateLobSql=updateLobSql+" `"+tc.getColumnName()+"` ,";
				listTableLobColumns.add(tc);
			}else{
				v_str=v_str+" ? ,";
			}
			for(String pk :listTablePKs){
				if(pk.equalsIgnoreCase(tc.getColumnName())){
					if(tc.getDataType().equalsIgnoreCase("ROWID")){
						where_sql=where_sql+" ROWIDTOCHAR("+pk+") = ? AND";
					}else{
						where_sql=where_sql+" `"+pk+"` = ? AND";
					}
				}
			}
		}
		insertSql=insertSql+"("+c_str.substring(0, c_str.length()-1)+")";
		insertSql=insertSql+" VALUES ("+v_str.substring(0, v_str.length()-1)+")";
		
		updateLobSql=updateLobSql.substring(0, updateLobSql.length()-1)+" FROM `"+table.getTargetTableName()+"` WHERE "+where_sql.substring(0, where_sql.length()-4)+" FOR UPDATE";
		_logger.info("--thread "+threadNum+" Insert SQL : "+insertSql);
		_logger.info("--thread "+threadNum+" Update LOB SQL : "+updateLobSql);
	}
	
	public void  getTableCons() throws Exception{
		Connection conn=sourceDataSource.getConnection();
		Statement stmt=conn.createStatement();
		/*String sql="select t.owner,t.table_name,c.COMMENTS from sys.all_all_tables t,sys.all_tab_comments c "+
					" where t.table_name=c.TABLE_NAME "+
					" and t.table_name='"+tableName+"' and t.owner ='"+owner+"' "+
					" order by t.owner,t.table_name";*/
		String sql="select ac.OWNER,ac.CONSTRAINT_NAME,ac.CONSTRAINT_TYPE,ac.TABLE_NAME,acc.COLUMN_NAME,acc.POSITION " +
				" from sys.all_constraints ac ,sys.all_cons_columns acc "  +
				" where ac.OWNER=acc.OWNER  "  +
				" and ac.CONSTRAINT_NAME=acc.CONSTRAINT_NAME "  +
				" and ac.TABLE_NAME='"+table.getTableName().toUpperCase()+"' "  +
				" and ac.owner ='"+owner.toUpperCase()+"'"+
				" and acc.POSITION IS NOT NULL "+
				" order by ac.CONSTRAINT_NAME, acc.POSITION";
		
		_logger.info("--thread "+threadNum+" "+sql);

		ResultSet rs=stmt.executeQuery(sql);
		while(rs.next()){
			_logger.info("--thread "+threadNum+" CONSTRAINT_NAME : "+rs.getString("CONSTRAINT_NAME"));
			if(rs.getString("CONSTRAINT_TYPE").equalsIgnoreCase("P")){
				listTablePKs.add(rs.getString("COLUMN_NAME"));
			}
		}
		_logger.info("--thread "+threadNum+" PK CONSTRAINT_NAME : "+listTablePKs);
		rs.close();
		stmt.close();
	}

}
