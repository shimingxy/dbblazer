/**
 * 
 */
package com.blazer.trans;

import java.util.ArrayList;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableDescribe;
import com.blazer.pipeline.PipeLineTask;
import com.blazer.trans.impl.*;


/**
 * 实现不同数据库数据的同步<br>
 * 根据数据源判断当前的的来源和去向数据库类型<br>
 * 
 * @author mhshi
 * @since 20180101
 * 
 */
public class TransData implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(TransData.class);
	DataSource sourceDataSource;
	DataSource targetDataSource;
	int  commitNum = 2000;
	int  threadSize =1;
	String fromUrl;
	String fromUser;
	String toUrl;
	String toUser;
	//<!-- FULL      清除表数据，然后全量插入  -->
	//<!-- INCREMENT 按条件进行删除，然后插入-->
	String transType="INCREMENT";
	String trans2DBType=null;
	String sourceDBType=null;
	
	ArrayList<TableDescribe> tablesList = new ArrayList<TableDescribe>();

	public int execute() throws Exception {
		_logger.info("-- --From URL " + fromUrl+" , USER "+fromUser);
		_logger.info("-- --To   URL " + toUrl+" , USER "+toUser);
		_logger.info("targetDataSource : "+targetDataSource.toString());
		if(sourceDataSource.toString().toLowerCase().indexOf("oracle")>-1) {
			sourceDBType="oracle";
		}else if(sourceDataSource.toString().toLowerCase().indexOf("mysql")>-1) {
			sourceDBType="mysql";
		}else if(sourceDataSource.toString().toLowerCase().indexOf("greenplum")>-1) {
			sourceDBType="greenplum";
		}
		
		
		_logger.info("targetDataSource : "+targetDataSource.toString());
		if(targetDataSource.toString().toLowerCase().indexOf("oracle")>-1) {
			trans2DBType="oracle";
		}else if(targetDataSource.toString().toLowerCase().indexOf("mysql")>-1) {
			trans2DBType="mysql";
		}else if(targetDataSource.toString().toLowerCase().indexOf("greenplum")>-1) {
			trans2DBType="greenplum";
		}
		
		int threadNum=tablesList.size()/threadSize;
		ArrayList<TableDescribe> threadTablesList=new ArrayList<TableDescribe>();
		int threadCount=1;
		for(TableDescribe tableQuery : tablesList){
			threadTablesList.add(tableQuery);
			if(threadTablesList.size()>=threadNum){
				runTranThread(sourceDataSource, targetDataSource, threadTablesList, transType, fromUser,
						commitNum, threadCount);
				threadCount++;
				threadTablesList=new ArrayList<TableDescribe>();
			}
		}
		if(threadTablesList.size()>0){
			runTranThread(sourceDataSource, targetDataSource, threadTablesList, transType, fromUser,
					commitNum, threadCount);

		}
		
		return 0;
	}
	
	public void  runTranThread(	DataSource sourceDataSource,
								DataSource targetDataSource,
								ArrayList<TableDescribe> listTables, 
								String transType,
								String owner,
								int commitNum,
								int threadNum) {
		Runnable transThread =null;
		
		if(sourceDBType.equalsIgnoreCase("oracle")) {
			if(trans2DBType.equals("oracle")) {
				transThread = new TransOracle2Oracle(
						sourceDataSource, targetDataSource, listTables, transType,fromUser, commitNum, threadNum);
			}else if(trans2DBType.equals("mysql")) {
				transThread = new TransOracle2MySql(
						sourceDataSource, targetDataSource, listTables, transType,fromUser, commitNum, threadNum);
			}else if(trans2DBType.equals("greenplum")) {
				transThread = new TransOracle2Greenplum(
						sourceDataSource, targetDataSource, listTables, transType,fromUser, commitNum, threadNum);
			}
		}
		
		if(sourceDBType.equalsIgnoreCase("greenplum")) {
			if(trans2DBType.equals("oracle")) {
				transThread = new TransGreenplum2Oracle(
						sourceDataSource, targetDataSource, listTables, transType,fromUser, commitNum, threadNum);
			}
		}
		
		Thread tt=new Thread(transThread);
		tt.start();
	}

	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}

	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}

	public DataSource getTargetDataSource() {
		return targetDataSource;
	}

	public void setTargetDataSource(DataSource targetDataSource) {
		this.targetDataSource = targetDataSource;
	}

	public int getCommitNum() {
		return commitNum;
	}

	public void setCommitNum(int commitNum) {
		this.commitNum = commitNum;
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

	public String getToUrl() {
		return toUrl;
	}

	public void setToUrl(String toUrl) {
		this.toUrl = toUrl;
	}

	public String getToUser() {
		return toUser;
	}

	public void setToUser(String toUser) {
		this.toUser = toUser;
	}

	public String getTransType() {
		return transType;
	}

	public void setTransType(String transType) {
		this.transType = transType;
	}

	public void setTablesList(ArrayList<TableDescribe> tablesList) {
		this.tablesList = tablesList;
	}
	
	
}
