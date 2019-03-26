package com.blazer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class ConnUtil
{


  public static Connection createConnection(String url, String user, String pwd, String dbType)
  {
	  Connection conn=null;
    try
    {
      if (dbType.equals("ORACLE")) {
        Class.forName("oracle.jdbc.OracleDriver");
      } else if (dbType.equals("MYSQL")) {
        Class.forName("com.mysql.jdbc.Driver");
      }else if (dbType.equals("DB2")) {
          Class.forName("com.ibm.db2.jcc.DB2Driver");
      }else if (dbType.equals("H2")) {
          Class.forName("org.h2.Driver");
      }
      
      
      
      conn = java.sql.DriverManager.getConnection(url, user, pwd);
      return conn;
    } catch (ClassNotFoundException e) {
      System.out.println("ClassNotFoundException");
      return null;
    } catch (SQLException e) {
      System.out.println("SQLException"); }
    return null;
  }
  





 

  public void releaseConnection(Connection conn)
  {
    if (conn != null) {
      try {
    	  conn.close();
      } catch (SQLException e) {
        System.out.println("SQLException");
      }
    }
  }
  
	public static void releaseConnection(Connection conn, java.sql.Statement stmt, PreparedStatement pstmt,ResultSet rs) {
		if (rs != null)
			try {
				rs.close();
				rs = null;
			} catch (SQLException e) {
				System.out.println("ResultSet Close Exception");
			}
		if (stmt != null)
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				System.out.println("Statement Close Exception");
			}
		if (pstmt != null)
			try {
				pstmt.close();
				pstmt = null;
			} catch (SQLException e) {
				System.out.println("PreparedStatement Close Exception");
			}
		if (conn != null) {
			try {
				conn.close();
				conn = null;
			} catch (SQLException e) {
				System.out.println("Connection Close Exception");
			}
		}
	}
	

  public static void releaseConnection(Connection conn,java.sql.Statement stmt, ResultSet rs)
  {
    if (rs != null)
      try {
        rs.close();
        rs = null;
      } catch (SQLException e) {
        System.out.println("SQLException");
      }
    if (stmt != null)
      try {
        stmt.close();
        stmt = null;
      } catch (SQLException e) {
        System.out.println("SQLException");
      }
    if (conn != null) {
      try {
    	  conn.close();
    	  conn = null;
      } catch (SQLException e) {
        System.out.println("SQLException");
      }
    }
  }
  
  public class DBTYPE
  {
    public static final String ORACLE = "ORACLE";
    public static final String MYSQL = "MYSQL";
    
    public DBTYPE() {}
  }
  
  public static void main(String[] args)  {
	  createConnection("jdbc:db2://127.0.0.1:50000/sample","db2admin","db2admin","DB2");
  }
}
