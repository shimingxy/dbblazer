package com.blazer.xlsx;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.TableColumns;
import com.blazer.db.TableConstraints;
import com.blazer.db.TableDescribe;
import com.blazer.pipeline.PipeLineTask;

/**
 * DB2 data definition language to Oralce
 * @author MHSHI
 *
 */
public class KzImportTask  implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(KzImportTask.class);
	DataSource dataSource;
	String url;
	String user;
	String owner;
	String importFilePath;
	FileOutputStream fop = null;
	ArrayList<TableDescribe>tableDescribeList=new ArrayList<TableDescribe>();
	HashMap<String, ArrayList<TableColumns>> tableColumnsMap = new HashMap<String, ArrayList<TableColumns>>();
	HashMap<String, ArrayList<TableConstraints>> pkColumnsMap = new HashMap<String, ArrayList<TableConstraints>>();
	SimpleDateFormat sdf_ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public int execute() throws Exception {
		
		File xlsxFilePath=new File(importFilePath);
		String[] filePathList=xlsxFilePath.list();
		for(String xlsxFileName :filePathList) {
			_logger.debug("===========filename "+xlsxFileName);
			if(xlsxFileName.startsWith("controlpath_")) {
				readFile(importFilePath+xlsxFileName);
			}
		}

		_logger.info("===========end ");
		return 0;
	}

	public void readFile(String xlsxName) throws Exception {
		Connection sourcConn=dataSource.getConnection();
		File excelFile=new File(xlsxName);
		FileInputStream fis= new  FileInputStream(excelFile);
		Workbook workbook=new XSSFWorkbook(fis);
		Sheet sheet=workbook.getSheetAt(0);
		Row row=sheet.getRow(0);
		String entName=row.getCell(0).toString().substring("Ŀ����ҵ��".length());
		_logger.info("====entName '"+entName+"',");
		/*Statement stmt=sourcConn.createStatement();
		ResultSet rs=stmt.executeQuery("SELECT COUNT(*) FROM GS_TEST_KZLJ WHERE TABLE_ENTNAME='"+entName+"'");
		int count=0;
		while (rs.next()) {
			count=rs.getInt(1);
		}
		rs.close();
		if(count==0)
		_logger.info("====entName '"+entName+"' , count "+count);
		*/
		//int i=0;
		/*
		Statement stmt=sourcConn.createStatement();
		String gx="�ɶ�";
		for (Row rows : sheet) {
			if(i==0) {i++;continue;}////������һ��
			
			if("�ɶ�".equals(rows.getCell(0).toString())) {////���������¼
				i++;continue;
			}
			if("����Ͷ��".equals(rows.getCell(0).toString())) {////���������¼
				gx="����Ͷ��";
				i++;continue;
			}
			
			stmt.execute("INSERT INTO GS_TEST_KZLJ (TABLE_ENTNAME,ENTNAME,SFGD,CZBL,SJJD,JDTYPE,TABLE_REGNO,REGCAP,HYML,CLDATE) VALUES("
					+"'"+entName+"',"
					+"'"+rows.getCell(0)+"',"
					+"'"+gx+"',"
					+"'"+rows.getCell(1)+"',"
					+"'"+rows.getCell(2)+"',"
					+"'"+rows.getCell(3)+"',"
					+"'"+rows.getCell(4)+"',"
					+"'"+rows.getCell(5)+"',"
					+"'"+rows.getCell(6)+"',"
					+"'"+rows.getCell(7)+"'"
					+")");
			_logger.info("==========="+rows.getCell(0)+","+rows.getCell(1)+","+rows.getCell(2)+","+rows.getCell(3)+","+rows.getCell(4)+","+rows.getCell(5)+","+rows.getCell(6)+","+rows.getCell(7));
		}
		stmt.close();*/
		sourcConn.close();
		workbook.close();
		fis.close();
	}


	

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
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


	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getImportFilePath() {
		return importFilePath;
	}

	public void setImportFilePath(String importFilePath) {
		this.importFilePath = importFilePath;
	}




	
	
}
