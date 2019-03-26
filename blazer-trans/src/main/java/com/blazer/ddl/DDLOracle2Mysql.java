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
import java.util.HashMap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.db.ConnUtil;
import com.blazer.db.TableColumns;
import com.blazer.db.TableDescribe;

/**
 * Oracle data definition language to Mysql
 * @author MHSHI
 *
 */
public class DDLOracle2Mysql  extends DDLExportBase{
	private static final Logger _logger = LoggerFactory.getLogger(DDLOracle2Mysql.class);

	ArrayList<TableDescribe>tableDescribeList=new ArrayList<TableDescribe>();
	HashMap<String, ArrayList<TableColumns>> tableColumnsMap = new HashMap<String, ArrayList<TableColumns>>();
	HashMap<String, ArrayList<String>> pkColumnsMap = new HashMap<String, ArrayList<String>>();

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
		
		for (TableDescribe td : tableDescribeList) {
			_logger.info("No." + ++i + " , TABLE NAME : " + td.getTableName());
			this.out.write(("-- --No." + i + " , TABLE NAME : " + td.getTableName() + "\n").getBytes());

			ArrayList<TableColumns> tableColumnsList = (ArrayList<TableColumns>) this.tableColumnsMap.get(td.getTableName());

			ArrayList<String> PKtableColumnsList = (ArrayList<String>) this.pkColumnsMap.get(td.getTableName());

			this.out.write(("DROP TABLE IF EXISTS `" + td.getTableName() + "`;\n").getBytes());
			this.out.write(("CREATE TABLE `" + td.getTableName() + "` (\n").getBytes());

			for (int ic = 0; ic < tableColumnsList.size(); ic++) {
				TableColumns tableColumns = (TableColumns) tableColumnsList.get(ic);
				String tableColumns_str = String.format("%-40s","`" + tableColumns.getColumnName() + "` ");
				if ("VARCHAR2".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "varchar(" + tableColumns.getDataLength() + ")";
				}else if ("CHAR".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "varchar(" + tableColumns.getDataLength() + ")";
				}else if ("NVARCHAR2".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "varchar(" + tableColumns.getDataLength() + ")";
				}else if ("UROWID".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "varchar(" + tableColumns.getDataLength() + ")";
				} else if ("NUMBER".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "DECIMAL(" + tableColumns.getDataLength() + "," + tableColumns.getDataScale() + ")";
				} else if ("FLOAT".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + "FLOAT(" + tableColumns.getDataLength() + "," + tableColumns.getDataScale() + ")";
				} else if ("LONG RAW".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + " longblob ";
				} else if ("DATE".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + " datetime ";
				} else if ("LONG".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + " LONG ";
				} else if ("BLOB".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + " BLOB ";
				} else if ("CLOB".equalsIgnoreCase(tableColumns.getDataType())) {
					tableColumns_str = tableColumns_str + " text ";
				}

				if (tableColumns.getNullAble().equalsIgnoreCase("N")) {
					tableColumns_str = tableColumns_str + " NOT NULL ";
				}

				if (("VARCHAR2".equalsIgnoreCase(tableColumns.getDataType()))
						|| ("CHAR".equalsIgnoreCase(tableColumns.getDataType()))
						|| ("NVARCHAR2".equalsIgnoreCase(tableColumns.getDataType()))) {
					if ((tableColumns.getDefaultValue() != null) && (!tableColumns.getDefaultValue().equals(""))
							&& (!tableColumns.getDefaultValue().equalsIgnoreCase("NULL"))) {
						tableColumns_str = tableColumns_str + " DEFAULT " + tableColumns.getDefaultValue();
					}
				} else if (("NUMBER".equalsIgnoreCase(tableColumns.getDataType()))
						|| ("FLOAT".equalsIgnoreCase(tableColumns.getDataType()))) {
					if ((tableColumns.getDefaultValue() != null) && (!tableColumns.getDefaultValue().equals(""))
							&& (!tableColumns.getDefaultValue().equalsIgnoreCase("NULL"))) {
						tableColumns_str = tableColumns_str + " DEFAULT " + tableColumns.getDefaultValue();
					}
				} else if ((!"LONG RAW".equalsIgnoreCase(tableColumns.getDataType()))
						&& ("DATE".equalsIgnoreCase(tableColumns.getDataType())) && (tableColumns.getDefaultValue() != null)
						&& (!tableColumns.getDefaultValue().equals(""))
						&& (!tableColumns.getDefaultValue().equalsIgnoreCase("NULL"))) {
					tableColumns_str = tableColumns_str + " DEFAULT  now() ";
				}

				if ( tableColumns.getColumnComments()!= null&&!tableColumns.getColumnComments().equals("")) {
					tableColumns_str = tableColumns_str + " COMMENT '"+ (tableColumns.getColumnComments()).replaceAll("\n", "").replaceAll("\r", "") + "' ";
				}

				if (ic != tableColumnsList.size() - 1) {
					tableColumns_str = tableColumns_str + ",";
				} else if ((PKtableColumnsList != null) && (PKtableColumnsList.size() > 0)) {
					tableColumns_str = tableColumns_str + ",";
				} else {
					//tableColumns_str = tableColumns_str;
				}

				this.out.write(tableColumns_str.getBytes());
				this.out.write("\n".getBytes());
			}

			String pk_tableColumns_str = "";
			if (PKtableColumnsList != null) {
				for (int ic = 0; ic < PKtableColumnsList.size(); ic++) {
					if (ic != PKtableColumnsList.size() - 1) {
						pk_tableColumns_str = pk_tableColumns_str + "`" + (String) PKtableColumnsList.get(ic) + "`,";
					} else {
						pk_tableColumns_str = pk_tableColumns_str + "`" + (String) PKtableColumnsList.get(ic) + "`";
					}
				}
			}

			if ((PKtableColumnsList != null) && (PKtableColumnsList.size() != 0)) {
				this.out.write(("PRIMARY KEY (" + pk_tableColumns_str + ")").getBytes());
			}
			this.out.write((") COMMENT='" +td.getTableComments()+"' "+ this.engine + " ; \n").getBytes());
			this.out.write("\n".getBytes());
		}
	}

	public void  getTable() throws Exception  {
		Connection conn = dataSource.getConnection();

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT UT.TABLE_NAME,TC.COMMENTS FROM USER_TABLES UT LEFT JOIN USER_TAB_COMMENTS TC ON UT.TABLE_NAME=TC.TABLE_NAME ORDER BY UT.TABLE_NAME");

		while (rs.next()) {
			tableDescribeList.add(new TableDescribe(user,rs.getString("TABLE_NAME"),rs.getString("COMMENTS")));
		}
		ConnUtil.releaseConnection(conn, stmt, rs);
	}

	public ArrayList<String> getPK(String TABLE_NAME) throws Exception {

		Connection conn = dataSource.getConnection();

		ArrayList<String> PKtableColumnsList = new ArrayList<String>();

		Statement stmt_pk = conn.createStatement();
		ResultSet rs_pk = stmt_pk
				.executeQuery("select * from user_cons_columns where Table_Name='" + TABLE_NAME + "' and POSITION>0");
		while (rs_pk.next()) {
			String PK_tableColumns_NAME = rs_pk.getString("COLUMN_NAME");
			PKtableColumnsList.add(PK_tableColumns_NAME);
		}

		ConnUtil.releaseConnection(conn, stmt_pk, rs_pk);

		return PKtableColumnsList;
	}

	public ArrayList<String> getPKAll() throws Exception {
		Connection conn = dataSource.getConnection();

		ArrayList<String> PKtableColumnsList = new ArrayList<String>();

		Statement stmt_pk = conn.createStatement();
		ResultSet rs_pk = stmt_pk.executeQuery("select * from user_cons_columns where  POSITION>0");
		String TABLE_NAME = "";
		int j = 0;
		while (rs_pk.next()) {
			String c_PK_tableColumns_NAME = rs_pk.getString("COLUMN_NAME");
			String c_TABLE_NAME = rs_pk.getString("Table_Name");

			if (!TABLE_NAME.equalsIgnoreCase(c_TABLE_NAME)) {
				if (!TABLE_NAME.equals("")) {
					this.pkColumnsMap.put(TABLE_NAME, PKtableColumnsList);
					j++;
					_logger.info("No." + j + " , TABLE " + TABLE_NAME + " , PKCOLUMN_NAMES : " + PKtableColumnsList);
					PKtableColumnsList = new ArrayList<String>();
				}
				TABLE_NAME = c_TABLE_NAME;
			}
			PKtableColumnsList.add(c_PK_tableColumns_NAME);
		}
		_logger.info("No." + ++j + " , TABLE " + TABLE_NAME + " , COLUMN_NAME : " + PKtableColumnsList);
		this.pkColumnsMap.put(TABLE_NAME, PKtableColumnsList);
		ConnUtil.releaseConnection(conn, stmt_pk, rs_pk);

		return PKtableColumnsList;
	}


	public ArrayList<TableColumns> getcolumnAll() throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt_c = conn.createStatement();
		ResultSet rs_c = stmt_c.executeQuery(
				"SELECT * FROM USER_TAB_COLUMNS TC LEFT JOIN USER_COL_COMMENTS TCC ON TC.TABLE_NAME=TCC.TABLE_NAME AND TC.COLUMN_NAME=TCC.COLUMN_NAME WHERE TC.TABLE_NAME IN(SELECT TABLE_NAME FROM USER_TABLES )   ORDER BY TC.TABLE_NAME, TC.COLUMN_ID");
		int j = 0;
		ArrayList<TableColumns> tableColumnsList = new ArrayList<TableColumns>();

		String TABLE_NAME = "";
		while (rs_c.next()) {
			String c_TABLE_NAME = rs_c.getString("Table_Name");
			TableColumns tableColumns = new TableColumns();
			tableColumns.setColumnName(rs_c.getString("COLUMN_NAME"));
			tableColumns.setDataType( rs_c.getString("DATA_TYPE"));
			tableColumns.setDataLength(rs_c.getInt("DATA_LENGTH"));
			tableColumns.setDataScale(rs_c.getInt("DATA_SCALE"));
			tableColumns.setDefaultValue( rs_c.getString("DATA_DEFAULT"));
			tableColumns.setNullAble(rs_c.getString("NULLABLE"));
			tableColumns.setColumnComments(rs_c.getString("COMMENTS"));

			if (!TABLE_NAME.equalsIgnoreCase(c_TABLE_NAME)) {
				if (!TABLE_NAME.equals("")) {
					this.tableColumnsMap.put(TABLE_NAME, tableColumnsList);
					j++;
					_logger.info("No." + j + " , TABLE " + TABLE_NAME + " , COLUMN_NAME : " + tableColumnsList);
					tableColumnsList = new ArrayList<TableColumns>();
				}
				TABLE_NAME = c_TABLE_NAME;
			}

			tableColumnsList.add(tableColumns);
		}

		_logger.info("No." +(++j) + " , TABLE " + TABLE_NAME + " , COLUMN_NAME : " + tableColumnsList);
		this.tableColumnsMap.put(TABLE_NAME, tableColumnsList);
		ConnUtil.releaseConnection(conn, stmt_c, rs_c);
		return tableColumnsList;
	}

	public ArrayList<String> getIndex(String TABLE_NAME) throws Exception {
		Connection conn = dataSource.getConnection();
		Statement stmt_c = conn.createStatement();
		String insql = "select cu.constraint_name from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P'";

		String sql = "select t.Index_Name,t.table_name,t.column_name,i.tablespace_name,i.uniqueness from user_ind_columns t,user_indexes i where t.index_name=i.index_name and t.table_name=i.table_name and i.index_name not in("
				+ insql + ")  ORDER BY Table_Name, Index_Name";

		ResultSet rs_c = stmt_c.executeQuery(sql);

		int i = 0;
		String table_name = "";
		String Index_Name = "";
		String column_names = "";
		String uniqueness = "";
		while (rs_c.next()) {
			String c_Index_Name = rs_c.getString("Index_Name");
			String c_table_name = rs_c.getString("table_name");
			String c_uniqueness = rs_c.getString("uniqueness");

			if (!c_Index_Name.equalsIgnoreCase(Index_Name)) {
				if (!Index_Name.equals("")) {
					writeIndex(uniqueness, Index_Name, table_name, column_names);

					_logger.info("-- ---- ---- ---- -----" + column_names);
				}

				uniqueness = c_uniqueness;
				column_names = "";

				_logger.info("No . " + (++i) + "  , c_table_name  : " + c_table_name + "  , Index_Name  : "
						+ c_Index_Name + "  , uniqueness  : " + uniqueness);
				this.out.write(("-- --No . " + i + "  , TABLE  : " + c_table_name + "  , INDEX  : " + c_Index_Name
						+ "  , uniqueness  : " + uniqueness + "\n").getBytes());
			}
			Index_Name = c_Index_Name;
			table_name = c_table_name;

			column_names = column_names + " `" + rs_c.getString("column_name") + "`,";
		}

		writeIndex(uniqueness, Index_Name, table_name, column_names);
		_logger.info("-- ---- ---- ---- -----" + column_names);
		ConnUtil.releaseConnection(conn, stmt_c, rs_c);
		_logger.info("finish INDEX");
		return null;
	}

	public void writeIndex(String uniqueness, String Index_Name, String table_name, String column_names)
			throws IOException {
		this.out.write("\n".getBytes());
		if (uniqueness.equalsIgnoreCase("UNIQUE")) {
			this.out.write(("CREATE  UNIQUE INDEX "+String.format("%-20s","`" + Index_Name + "`")+" ON `" + table_name + "` ("
					+ column_names.substring(0, column_names.length() - 1) + ")  ;\n").getBytes());
		} else {
			this.out.write(("CREATE  INDEX "+String.format("%-20s","`" + Index_Name + "`")+" ON `" + table_name + "` ("
					+ column_names.substring(0, column_names.length() - 1) + ")  ;\n").getBytes());
		}
	}


	
}
