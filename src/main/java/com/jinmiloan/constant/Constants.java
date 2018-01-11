package com.jinmiloan.constant;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * 项目配置相关的常量
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String SPARK_LOCAL = "spark.local";
	String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
	String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
	String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";
	String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
	String KAFKA_TOPICS = "kafka.topics";
	
	/**
	 * Spark作业相关的常量
	 */

	
	/**
	 * 任务相关的常量
	 */
	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";
	String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";
	
}
