package com.jinmiloan.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.jinmiloan.dao.insert_or_updateDAO;
import com.jinmiloan.domain.CountResult;
import com.jinmiloan.domain.dateCount;
import com.jinmiloan.jdbc.JDBCHelper;

public class insert_or_updateDAOImpl implements insert_or_updateDAO {

	@Override
	public void updateBatch(List<dateCount> dateCounts) {
		JDBCHelper jdbcHelper = new JDBCHelper();
		
		// 首先进行分类，分成待插入的和待更新的
		List<dateCount> insertCounts = new ArrayList<dateCount>();
		List<dateCount> updateCounts = new ArrayList<dateCount>();
		
		String selectSQL = "SELECT count(*) FROM JinmiloanTest "
				+ "WHERE time=? ";
		
		Object[] selectParams = null;
		
		for(dateCount dc : dateCounts){
			final CountResult cr=new  CountResult();
			selectParams = new Object[]{dc.getTime()};
			
			jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						cr.setCount(count);  
					}
				}
			});
				int count = cr.getCount();
				if(count > 0) {
					updateCounts.add(dc);
				} else {
					insertCounts.add(dc);
				}
				}
		
			// 执行批量插入
			String insertSQL = "INSERT INTO JinmiloanTest VALUES(?,?)";  
			List<Object[]> insertParamsList = new ArrayList<Object[]>();
			
			for(dateCount ic : insertCounts) {
				Object[] insertParams = 
						new Object[]{
						ic.getTime(),
						ic.getSum_userid()
											};  
				insertParamsList.add(insertParams);
				
			}

			jdbcHelper.executeBatch(insertSQL, insertParamsList);
			
			
			// 执行批量更新
			String updateSQL = "UPDATE JinmiloanTest SET sum_userid=sum_userid+? WHERE time=? ";  
			List<Object[]> updateParamsList = new ArrayList<Object[]>();
			
			for(dateCount uc : updateCounts) {
				Object[] updateParams = new Object[]{
						uc.getSum_userid(),
						uc.getTime()
						};  
				updateParamsList.add(updateParams);
			}

			jdbcHelper.executeBatch(updateSQL, updateParamsList);
		}
			
	}
