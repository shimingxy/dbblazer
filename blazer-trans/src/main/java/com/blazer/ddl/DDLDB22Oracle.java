package com.blazer.ddl;



import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.ConnUtil;
import com.blazer.db.TableColumns;
import com.blazer.db.TableDescribe;

/**
 * DB2 data definition language to Oralce
 * @author MHSHI
 *
 */
public class DDLDB22Oracle extends DDLExportBase{
	private static final Logger _logger = LoggerFactory.getLogger(DDLDB22Oracle.class);

	ArrayList<TableDescribe>tableDescribeList=new ArrayList<TableDescribe>();
	

	public int execute() throws Exception {
		File file = new File(this.exportFilePath);
		this.out = new FileOutputStream(file);

		if (!file.exists()) {
			file.createNewFile();
		}
		this.out.write(("-- --FROM SERVER " + this.url + "\n").getBytes());
		this.out.write(("-- --FROM USER " + this.user + "\n").getBytes());
		this.out.write(("-- --TO FILE " + this.exportFilePath + "\n").getBytes());
		this.out.write(("-- --TO engine " + this.engine + "\n").getBytes());
		this.out.write("-- --TABLE\n".getBytes());
		this.out.write(("-- --" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n").getBytes());
		getcolumnAll();
		getPKAll();
		build();
		this.out.write("-- --INDEX\n".getBytes());
		this.out.write(("-- --" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n").getBytes());
		getIndex("");

		this.out.write("-- --END\n".getBytes());
		this.out.write(("-- --" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n").getBytes());

		this.out.flush();
		this.out.close();
		return 0;
	}

	public void build() throws Exception {
		int i = 0;
		getTable();
		
		//getcol_comments();
		
		for (TableDescribe tableDescribe : tableDescribeList) {
			_logger.info("No." + (++i) + " , TABLE NAME : " + tableDescribe.getTableName());
			this.out.write(("-- --No." + i + " , TABLE NAME : " + tableDescribe.getTableName() + "\n").getBytes());

			ArrayList<TableColumns> tableColumnsList =tableDescribe.getTableColumnsList();

			//this.out.write(("DROP TABLE IF EXISTS " + tableDescribe.getTableName() + ";\n").getBytes());
			this.out.write(("DROP   TABLE " + tableDescribe.getTableName() + ";\n").getBytes());
			this.out.write(("CREATE TABLE " + tableDescribe.getTableName() + " (\n").getBytes());
			StringBuffer sbComments=new StringBuffer("");
			for (int ic = 0; ic < tableColumnsList.size(); ic++) {
				TableColumns COLUMN = (TableColumns) tableColumnsList.get(ic);
				
				String COLUMN_STR = "" + String.format("%-15s", COLUMN.getColumnName()) + " ";
				if ("VARCHAR".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " VARCHAR2(" + COLUMN.getDataLength() + ") ";
				}else if ("CHARACTER".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " CHAR(" + COLUMN.getDataLength() + ") ";
				}else if ("LONG VARCHAR".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " VARCHAR2(4000) ";
				}/*else if ("NVARCHAR2".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " VARCHAR2(" + COLUMN.getDataLength() + ")";
				}else if ("UROWID".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " VARCHAR2(" + COLUMN.getDataLength() + ")";
				}else if ("LONG RAW".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " longblob ";
				}*/
				else if ("DATE".equalsIgnoreCase(COLUMN.getDataType())
						 ||"TIME".equalsIgnoreCase(COLUMN.getDataType())
						 ||"TIMESTAMP".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " DATE ";
				} else if ("BLOB".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " BLOB ";
				} else if ("CLOB".equalsIgnoreCase(COLUMN.getDataType())
						 ||"GRAPHIC".equalsIgnoreCase(COLUMN.getDataType())
						 ||"VARGRAPHIC".equalsIgnoreCase(COLUMN.getDataType())
						 ||"LONG VARGRAPHIC".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " CLOB ";
				} else if ("DBCLOB".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NCLOB ";
				} else if ("SMALLINT".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(5,0) ";
				} else if ("INTEGER".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(10,0) ";
				} else if ("BIGINT".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(19,0) ";
				} else if ("FLOAT".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " FLOAT(" + COLUMN.getDataLength() + "," + COLUMN.getDataScale() + ")";
				} else if ("REAL".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(63,0) ";
				} else if ("DOUBLE".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(19,0) ";
				} else if ("DECIMAL".equalsIgnoreCase(COLUMN.getDataType())
						 ||"NUMBER".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " NUMBER(" + COLUMN.getDataLength() + "," + COLUMN.getDataScale() + ")";
				} else if ("XML".equalsIgnoreCase(COLUMN.getDataType())) {
					COLUMN_STR = COLUMN_STR + " XML ";
				}

				if (("VARCHAR".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("CHARACTER".equalsIgnoreCase(COLUMN.getDataType()))
						) {
					if ((COLUMN.getDefaultValue() != null) && (!COLUMN.getDefaultValue().equals(""))
							&& (!COLUMN.getDefaultValue().equalsIgnoreCase("NULL"))) {
						COLUMN_STR = COLUMN_STR + " DEFAULT " + COLUMN.getDefaultValue();
					}
				} else if (("NUMBER".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("SMALLINT".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("INTEGE".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("BIGINT".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("FLOAT".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("DOUBLE".equalsIgnoreCase(COLUMN.getDataType()))
						|| ("DECIMAL".equalsIgnoreCase(COLUMN.getDataType()))) {
					if ((COLUMN.getDefaultValue() != null) && (!COLUMN.getDefaultValue().equals(""))
							&& (!COLUMN.getDefaultValue().equalsIgnoreCase("NULL"))) {
						COLUMN_STR = COLUMN_STR + " DEFAULT " + COLUMN.getDefaultValue();
					}
				} else if (("DATE".equalsIgnoreCase(COLUMN.getDataType())
						 ||"TIME".equalsIgnoreCase(COLUMN.getDataType())
						 ||"TIMESTAMP".equalsIgnoreCase(COLUMN.getDataType()))&& (COLUMN.getDefaultValue() != null)
						&& (!COLUMN.getDefaultValue().equals(""))
						&& (!COLUMN.getDefaultValue().equalsIgnoreCase("NULL"))) {
					COLUMN_STR = COLUMN_STR + " DEFAULT  now() ";
				}
				
				if (COLUMN.getNullAble().equalsIgnoreCase("N")) {
					COLUMN_STR = COLUMN_STR + " NOT NULL ";
				}
				
				if (ic != tableColumnsList.size() - 1) {
					COLUMN_STR = COLUMN_STR + ",";
				} else {
					//COLUMN_STR = COLUMN_STR;
				}

				this.out.write(COLUMN_STR.getBytes());
				this.out.write("\n".getBytes());
				//Column Comments
				if(COLUMN.getColumnComments()!=null&&!COLUMN.getColumnComments().equals("")) {
					sbComments.append("COMMENT ON COLUMN "+String.format("%-40s", tableDescribe.getTableName()+"."+COLUMN.getColumnName())+" IS '"+COLUMN.getColumnComments()+"';\n");
				}
			}

			
			this.out.write((")" + this.engine + " ; \n").getBytes());
			
			//PRIMARY KEY
			if (!(tableDescribe.getConstraints().getConstraintMap().isEmpty())) {
				for(String constraint :tableDescribe.getConstraints().getConstraintMap().keySet()) {
					if(tableDescribe.getConstraints().getConstraintType(constraint).equalsIgnoreCase("primary")) {
						String consColumnString="";
						int consColumnCount=0;
						for (String consColumn : tableDescribe.getConstraints().getConstraint(constraint)) {
							if(consColumnCount==0) {
								consColumnString=consColumn;
							}else {
								consColumnString=consColumnString+" , "+consColumn;
							}
							consColumnCount++;
						}
						this.out.write(("\n").getBytes());
						this.out.write(("ALTER TABLE "+String.format("%-20s", tableDescribe.getTableName())+" ADD CONSTRAINT "+String.format("%-20s", constraint)+" PRIMARY KEY (" + consColumnString + ") ; \n").getBytes());
				
					}
				}
			}
			//Table Comments
			if(tableDescribe.getTableComments() !=null &&!tableDescribe.getTableComments().equals("")) {
				this.out.write(("\n").getBytes());
				this.out.write(("COMMENT ON TABLE "+String.format("%-20s", tableDescribe.getTableName())+" IS '"+tableDescribe.getTableComments()+"' ; \n").getBytes());
			}
			
			//Column Comments write
			if(sbComments.length()>10) {
				this.out.write(("\n"+sbComments.toString()).getBytes());
			}
			
			this.out.write("\n".getBytes());
		}
	}

	public void getTable() throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT   *   FROM   SYSCAT.TABLES  WHERE \"TYPE\"='T' AND TABSCHEMA='"+this.owner+"' ORDER BY  TABNAME");

		while (rs.next()) {
			
			tableDescribeList.add(new TableDescribe(this.owner,rs.getString("TABNAME"),rs.getString("REMARKS")));
		}

		ConnUtil.releaseConnection(conn, stmt, rs);
	}


	public ArrayList<String> getPK(String TABLE_NAME) throws Exception {

		Connection conn = dataSource.getConnection();

		ArrayList<String> PK_TableColumnsList = new ArrayList<String>();

		Statement stmt_pk = conn.createStatement();
		ResultSet rs_pk = stmt_pk
				.executeQuery("select * from user_cons_columns where Table_Name='" + TABLE_NAME + "' and POSITION>0");
		while (rs_pk.next()) {
			String PK_COLUMN_NAME = rs_pk.getString("COLUMN_NAME");
			PK_TableColumnsList.add(PK_COLUMN_NAME);
		}

		ConnUtil.releaseConnection(conn, stmt_pk, rs_pk);

		return PK_TableColumnsList;
	}

	public void getPKAll() throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt_pk = conn.createStatement();
		ResultSet rs_pk = stmt_pk.executeQuery("SELECT * FROM SYSCAT.INDEXES WHERE TABSCHEMA='"+this.owner+"'  AND UNIQUERULE='P' ORDER BY TABNAME, INDNAME");
		//String TABLE_NAME = "";
		int j = 0;
		while (rs_pk.next()) {
			String c_colNames=rs_pk.getString("COLNAMES");
			String c_tableName = rs_pk.getString("TABNAME");
			String c_ConstName = rs_pk.getString("INDNAME");
			_logger.info("No." + j + " , TABLE " + c_tableName +" , CONSTRAINT "+c_ConstName+ " , PKCOLUMNS : " + c_colNames);
			
			for (TableDescribe tableDescribe : tableDescribeList) {
				if(tableDescribe.getTableName().equalsIgnoreCase(c_tableName)) {
					tableDescribe.getConstraints().addConstraint(c_ConstName, c_colNames.substring(1).replace("+", ","));
					tableDescribe.getConstraints().setConstraintType(c_ConstName, "primary");
				}
			}

		}
		ConnUtil.releaseConnection(conn, stmt_pk, rs_pk);
	}
	
	public void getcolumnAll() throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt_c = conn.createStatement();
		ResultSet rs_c = stmt_c.executeQuery(
				"SELECT * FROM SYSCAT.COLUMNS  WHERE TABNAME IN(SELECT   TABNAME   FROM   SYSCAT.TABLES  WHERE \"TYPE\"='T' AND TABSCHEMA='"+this.owner+"') ORDER BY TABNAME, COLNO");
		int j = 0;
		TableDescribe tableDescribe=new TableDescribe();

		String TABLE_NAME = "";
		while (rs_c.next()) {
			String c_TABLE_NAME = rs_c.getString("TABNAME");
			TableColumns COLUMN = new TableColumns();
			COLUMN.setColumnName(rs_c.getString("COLNAME"));
			COLUMN.setDataType( rs_c.getString("TYPENAME"));
			COLUMN.setDataLength(rs_c.getInt("LENGTH"));
			COLUMN.setDataScale(rs_c.getInt("SCALE"));
			COLUMN.setDefaultValue( rs_c.getString("DEFAULT"));
			COLUMN.setNullAble(rs_c.getString("NULLS"));
			COLUMN.setColumnComments(rs_c.getString("REMARKS"));

			if (!TABLE_NAME.equalsIgnoreCase(c_TABLE_NAME)) {
				if (!TABLE_NAME.equals("")) {
					j++;
					tableDescribe.setTableName(TABLE_NAME);
					tableDescribeList.add(tableDescribe);
					_logger.info("No." + j + " , TABLE " + TABLE_NAME + " , COLUMNS : " + tableDescribe.getTableColumnsList());
					 tableDescribe=new TableDescribe();
				}
				TABLE_NAME = c_TABLE_NAME;
				tableDescribe.setTableName(TABLE_NAME);
			}

			tableDescribe.getTableColumnsList().add(COLUMN);
		}

		_logger.info("No." +(++j) + " , TABLE " + TABLE_NAME + " , COLUMNS : " + tableDescribe.getTableColumnsList());
		//this.tableColumnsMap.put(TABLE_NAME, tableColumnsList);
		ConnUtil.releaseConnection(conn, stmt_c, rs_c);
		
	}

	public ArrayList<String> getIndex(String TABLE_NAME) throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt_c = conn.createStatement();

		String sql = "SELECT TABNAME, INDNAME,* FROM SYSCAT.INDEXES WHERE TABSCHEMA='"+this.owner+"'  AND UNIQUERULE='U' ORDER BY TABNAME, INDNAME";

		ResultSet rs_c = stmt_c.executeQuery(sql);

		int i = 0;
		String table_name = "";
		String Index_Name = "";
		String column_names = "";
		String uniqueness = "";
		while (rs_c.next()) {
			 Index_Name = rs_c.getString("INDNAME");
			 table_name = rs_c.getString("TABNAME");
			 uniqueness = rs_c.getString("UNIQUERULE");
			 column_names = rs_c.getString("COLNAMES");
			 column_names=column_names.substring(1);
			 column_names=column_names.replace("+", ",");
			_logger.info("No . " + (++i) + "  , c_table_name  : " + table_name + "  , Index_Name  : "+ Index_Name + "  , uniqueness  : " + uniqueness);
			this.out.write("\n".getBytes());
			this.out.write(("-- --No . " + i + "  , TABLE  : " + table_name + "  , INDEX  : " + Index_Name+ "  , uniqueness  : " + uniqueness + "\n").getBytes());
			writeIndex(uniqueness, Index_Name, table_name, column_names);
		}

		//writeIndex(uniqueness, Index_Name, table_name, column_names);
		_logger.info("-- ---- ---- ---- -----" + column_names);
		ConnUtil.releaseConnection(conn, stmt_c, rs_c);
		_logger.info("finish INDEX");
		return null;
	}

	public void writeIndex(String uniqueness, String Index_Name, String table_name, String column_names)
			throws IOException {
		
		if (uniqueness.equalsIgnoreCase("UNIQUE")) {
			this.out.write(("CREATE  UNIQUE INDEX " + String.format("%-20s", Index_Name )+ " ON " + String.format("%-20s", table_name) + " ("+ column_names + ")  ;\n").getBytes());
		} else {
			this.out.write(("CREATE  INDEX " +  String.format("%-20s", Index_Name ) + " ON " + String.format("%-20s", table_name )+ " ("+ column_names + ")  ;\n").getBytes());
		}
	}

}
