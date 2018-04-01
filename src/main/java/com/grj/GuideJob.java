package com.grj;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.grj.mapreduce.GetRecostMapper;
import com.grj.mapreduce.GetRecostReducer;
import com.grj.mapreduce.MergeMapper;
import com.grj.mapreduce.MergeReducer;
import com.grj.util.Constants;
import com.grj.util.HbaseUtil;
import com.grj.util.RedisUtil;
import com.grj.util.ShellUtil;

public class GuideJob implements org.quartz.Job {

	public static Logger logger = LoggerFactory.getLogger(GuideJob.class);

	/**
	 * springdata 和 redis 整合所用的配置文件,加入spring容器中
	 */
	public static ClassPathXmlApplicationContext applicationContext = 
			new ClassPathXmlApplicationContext("applicationContext.xml");
	
	/**
	 * 这个方法要在com.grj.Main里进行使用
	 */
	@Deprecated
	public static void main(String[] args) {

		/*SchedulerFactory sf = new StdSchedulerFactory();
		Scheduler sched;
		
		try {
			sched = sf.getScheduler();
			Date runTime = evenMinuteDate(new Date());

			JobDetail job = newJob(SimpleTimingScheduleTest.class)
					.withIdentity("job1", "group1").build();


			CronTrigger trigger = newTrigger()
					.withIdentity("trigger1", "group1")
					.withSchedule(CronScheduleBuilder.cronSchedule(Constants.CRONSCHEDULE))
			        .build();

			sched.scheduleJob(job, trigger);

			sched.start();

			Thread.sleep(100L * 1000L);
			sched.shutdown(true);
			
		} catch (SchedulerException | InterruptedException e) {
			e.printStackTrace();
		}*/
		
		try {
			new GuideJob().execute(null);
		} catch (JobExecutionException e) {
			e.printStackTrace();
		}

		
	}

	
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		GuideJob GuideJob = new GuideJob();

		try {
			// step1:执行mapreduce 初级计算
			GuideJob.runGetRecostMapReduce();
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		try {
			// step2:合并计算结果存入hbase
			GuideJob.runMergeMapReduce();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			// step3:加入redis
			GuideJob.loadDataToRedis();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			// step4:通过sqoop把计算指标放入mysql
			GuideJob.saveResultToMysql();
		} catch (IOException e) {
			e.printStackTrace();
		}	

		
	}

	/**
	 *  Import HBase data into Redis.
	 */
	@SuppressWarnings("unused")
	private void loadDataToRedis() throws IOException {
		//get a connection to hbase
		Connection connection = HbaseUtil.getConnection();
		
		//get a table from hbase by connection
		Table table = connection.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
		
		/*
		 * packaging a Scan Object
		 * get a ResultScanner
		 */
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);

		/*
		 * 
		 */
		Iterator<Result> iterator = resultScanner.iterator();
		while (iterator.hasNext()) {
			Result result = iterator.next();
			byte[] row = result.getRow();
			List<Cell> listCells = result.listCells();
			HashMap<String, String> info = new HashMap<String, String>();
			// info.put("rowkey", row);
			for (int i = 0; i < listCells.size(); i++) {
				// cell 对应 列 与 值
				Cell cell = listCells.get(i);
				// 拿到列名
				byte[] qualifier = cell.getQualifier();
				// 拿到列值
				byte[] value = cell.getValue();
				// 把不同类型的字节数组转化成字符串
				String valueString = changeFieldToString(qualifier, value);
				// 用map 存储信息
				info.put(Bytes.toString(qualifier), valueString);
			}
			RedisUtil.save(Bytes.toString(row), info);
		}

	}

	
	/**
	 * Change the value 成String类型 according to the qualifier.
	 */
	private String changeFieldToString(byte[] qualifier, byte[] value) {
		String colName = Bytes.toString(qualifier);
		if (colName.equals("hcount") || colName.equals("ocount")) {
			return String.valueOf(Bytes.toInt(value));
		} else if(colName.equals("hcost") || colName.equals("ocost")){
			return String.valueOf(Bytes.toDouble(value));
		}else{
			return Bytes.toString(value);
		}
	}

	
	/**
	 * Import HDFS data into mysql via sqoop1.
	 */
	private void saveResultToMysql() throws IOException {
		logger.info("-------------------Import HDFS data into mysql via sqoop1.");
		//计算结果保存在hdfs上  sqoop安装在远程linux上  因此需要远程连接linux，执行shell脚本
		
		/*
		 * create a table guide_hospital:
		 * 
		 * create table guide_hospital(
		 * 	hospital_id varchar(32) primary key 
		 * 	,avghcost double(10,2)
		 * 	,avgocost double(10,2)
		 * );
		 */

		/*
		 * Sqoop script:
		 * 
		 * sqoop export -m 1 
		 * --connect jdbc:mysql://localhost:3306/guide 
		 * --username root 
		 * --password 123456 
		 * --table guide_hospital 
		 * --export-dir /guide/merge/part-r-00000 
		 * --input-fields-terminated-by '\t' 
		 * --mysql-delimiters
		 */
		String result = ShellUtil.execute(
				Constants.MASTER_HOST
				,Constants.MASTER_USERNAME
				,Constants.MASTER_PASSWORD
				,Constants.MASTER_SHELL_SCRIPT);
		
		System.out.println(result);
	}

	
	/**
	 * 合并(record+reimbusers)的数据
	 */
	@SuppressWarnings("unused")
	private void runMergeMapReduce() 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, Constants.JOB_MERGE_NAME);
		//
		job.setJarByClass(GuideJob.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 设置输出key 和 value 的类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置输入路径
		FileInputFormat.addInputPath(job, new Path(Constants.JOB_MERGE_INPUT_PATH));
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		Path outputPath = new Path(Constants.JOB_MERGE_OUTPUT_PATH);
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);		
	}

	
	
	/**
	 * 关联(record+reimbuser)数据
	 */
	@SuppressWarnings("unused")
	private void runGetRecostMapReduce() 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		logger.info("----------------------------------------"+"runGetRecostMapReduce");
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, Constants.JOB_GETRECOST_NAME);
		//
		job.setJarByClass(GuideJob.class);
		job.setMapperClass(GetRecostMapper.class);
		job.setReducerClass(GetRecostReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 设置输出key 和 value 的类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置输入 输出路径
		FileInputFormat.addInputPath(job, new Path(Constants.JOB_GETRECOST_INPUT_PATH));
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		Path outputPath = new Path(Constants.JOB_GETRECOST_OUTPUT_PATH);
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);

		//
		job.waitForCompletion(true);

	}

}
