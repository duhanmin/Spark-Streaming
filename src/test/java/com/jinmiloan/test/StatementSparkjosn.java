package com.jinmiloan.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.jinmiloan.dao.insert_or_updateDAO;
import com.jinmiloan.dao.factory.DAOFactory;
import com.jinmiloan.domain.dateCount;

/**
 * 解析实时josn数据并传入数据库
 * @author Administrator
 *
 */
public class StatementSparkjosn {


	public static void main(String[] args) throws IOException {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("StatementSparkjosn")
//				;
				.setMaster("local[4]");

		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(3));
		final HiveContext hiveContext = new HiveContext(sc.sc());
		
		Configuration configuration = sc.hadoopConfiguration();
		Job job = Job.getInstance(configuration,"StatementSparkjosn");
		job.setJarByClass(StatementSparkjosn.class);
		
		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "hadoop71:9092,hadoop72:9092,hadoop73:9092");
		
		// 然后，要创建一个set，里面放入，你要读取的topic
		// 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
		Set<String> topics = new HashSet<String>();
		topics.add("nginxLogs");
		// 创建输入DStream
		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);
			
		
		lines.foreachRDD(new Function<JavaPairRDD<String,String>, Void>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				 if(v1.count() == 0)  return null ;

				 DataFrame JinmiloanMysql = hiveContext.read().format("json").json(v1.values());  

				// （注册临时表，针对临时表执行sql语句）
				JinmiloanMysql.registerTempTable("JinmiloanMysql");
				
				String sql="select "
											+ "time , COUNT(userid) as sum_userid "
							+ "from"
								       			    + " (select "
												    		+ " substr(create_time,1,10) as time , userid "
												    + "from "
															+ "JinmiloanMysql) tb"
							 + " group by time";
				
				DataFrame JinmiloanMysqlDF = hiveContext.sql(sql);
				JinmiloanMysqlDF.show();
				JavaRDD<Row> JinmiloanMysqlRDD = JinmiloanMysqlDF.javaRDD();
				
				JavaRDD<String> multipleNumberRDD = JinmiloanMysqlRDD.map(
						new Function<Row, String>() {
							private static final long serialVersionUID = 1L;
									@Override
									public String call(Row row) throws Exception {
											StringBuffer bf =new StringBuffer();
											String[] strings = row.toString().split(",");
											
												bf.append(strings[0]+",")
												.append(strings[1]);
												return String.valueOf(bf);	
									}
								});
				
				
				//	写数据库
				multipleNumberRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<String> iterator) throws Exception {
						List<dateCount> crs = new ArrayList<dateCount>();		
						
						while (iterator.hasNext()) {
							dateCount cr=new  dateCount();
							
							String str = (String) iterator.next();

							String[] Splited = str.substring(1, str.length()-1).split(",");
							
							if ((!Splited[0].equals("") || !Splited[0].equals(null) ) ||
												(!Splited[1].equals("") || !Splited[1].equals(null) )){
								
								// yyyy-MM-dd
								String time = Splited[0];
								int sum_userid = Integer.valueOf(Splited[1]);
								cr.setSum_userid(sum_userid);
								cr.setTime(time);
	
								crs.add(cr);
							}
						}
						
						if( crs.size() != 0 && (!crs.get(0).getTime().equals(null) || !crs.get(0).getTime().equals("")) ) {
							insert_or_updateDAO iouDAO = DAOFactory.getinsert_or_updateDAO();
							iouDAO.updateBatch(crs);
						}
					}
				});
				return null;
			}
		});
		

		// 关闭JavaSparkContext
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
