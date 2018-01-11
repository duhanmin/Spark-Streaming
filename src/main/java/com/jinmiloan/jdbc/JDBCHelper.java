package com.jinmiloan.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.jinmiloan.util.C3P0JDBCUtils;


/**
 * 增删改查的方法
 * 1、执行增删改SQL语句的方法
 * 2、执行查询SQL语句的方法
 * 3、批量执行SQL语句的方法
 */

public class JDBCHelper {
	public static C3P0JDBCUtils dsu=new C3P0JDBCUtils();
	
	/**
	 * 执行增删改SQL语句
	 * @param sql 
	 * @param params
	 * @return 影响的行数
	 */
	@SuppressWarnings("static-access")
	public int executeUpdate(String sql, Object[] params) {
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn =dsu.getCurrentConnection();
			conn.setAutoCommit(false);  
			
			pstmt =conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);  
				}
			}
			
			rtn = pstmt.executeUpdate();
			
		} catch (Exception e) {
			try {
				dsu.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			if(conn != null) {
				try {
					dsu.commitAndRelease();
				} catch (SQLException e) {
					e.printStackTrace();
				}  
			}
		}
		
		return rtn;
	}
	

	/**
	 * 执行查询SQL语句
	 * @param sql
	 * @param params
	 * @param callback
	 */
	@SuppressWarnings("static-access")
	public void executeQuery(String sql, Object[] params, 
			QueryCallback callback) {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn =dsu.getCurrentConnection();
			pstmt = conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);   
				}
			}
			
			rs = pstmt.executeQuery();
			
			callback.process(rs);  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					dsu.AndRelease();
				} catch (SQLException e) {
					e.printStackTrace();
				}  
			}
		}
	}
	
	/**
	 * 批量执行SQL语句
	 * 
	 * 批量执行SQL语句，是JDBC中的一个高级功能
	 * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
	 * 
	 * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
	 * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
	 * 都要向MySQL发送一次网络请求
	 * 
	 * 可以通过批量执行SQL语句的功能优化这个性能
	 * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
	 * 执行的时候，也仅仅编译一次就可以
	 * 这种批量执行SQL语句的方式，可以大大提升性能
	 * 
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL语句影响的行数
	 */
	@SuppressWarnings("static-access")
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			
			conn =dsu.getCurrentConnection();
			
			// 第一步：使用Connection对象，取消自动提交
			conn.setAutoCommit(false);  
			
			pstmt = conn.prepareStatement(sql);
			
			// 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
			if(paramsList != null && paramsList.size() > 0) {
				for(Object[] params : paramsList) {
					for(int i = 0; i < params.length; i++) {
						pstmt.setObject(i + 1, params[i]);  
					}
					pstmt.addBatch();
				}
			}
			
			// 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
			rtn = pstmt.executeBatch();

		} catch (Exception e) {
			try {
				dsu.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			if(conn != null) {
				try {
					dsu.commitAndRelease();
				} catch (SQLException e) {
					e.printStackTrace();
				}  
			}
		}
		
		return rtn;
	}
	
	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback {
		
		/**
		 * 处理查询结果
		 * @param rs 
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
		
	}
	
}
