package com.jinmiloan.dao.factory;

import com.jinmiloan.dao.insert_or_updateDAO;
import com.jinmiloan.dao.impl.insert_or_updateDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {

	public static insert_or_updateDAO getinsert_or_updateDAO() {
		return new insert_or_updateDAOImpl();
	}
	
}
