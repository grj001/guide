package com.grj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.grj.mapreduce.GetRecostMapper;
import com.grj.mapreduce.GetRecostReducer;
import com.grj.mapreduce.MergeMapper;
import com.grj.mapreduce.MergeReducer;
import com.grj.util.Constants;

public class StartMain {

	public static Logger logger = LoggerFactory.getLogger(StartMain.class);

	public static void main(String[] args) {
		StartMain startMain = new StartMain();
		// step1:执行mapreduce 初级计算
		/*try {
			startMain.runGetRecostMapReduce();
		} catch (Exception e) {
			logger.info(e.getMessage());
			e.printStackTrace();
		}*/

		
		//startMain.loadDataToRedis();
		// step2:合并计算结果存入hbase
		try {
			startMain.runMergeMapReduce();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// step3:加入redis
			
		// step4:通过sqoop把计算指标放入mysql
//		startMain.saveResultToMysql();

	}

	private void saveResultToMysql() {
		// TODO Auto-generated method stub
		System.out.println("save data to mysql");
	}

	private void runMergeMapReduce() 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "save data to hbase");
		//
		job.setJarByClass(StartMain.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 设置输出key 和 value 的类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置输入路径
		FileInputFormat.addInputPath(job, new Path("/guide/out/"));
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		Path outputPath = new Path("/guide/outnull/");
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);		
	}

	@SuppressWarnings("deprecation")
	private void runGetRecostMapReduce() 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, Constants.JOB_NAME);
		//
		job.setJarByClass(StartMain.class);
		job.setMapperClass(GetRecostMapper.class);
		job.setCombinerClass(GetRecostReducer.class);
		job.setReducerClass(GetRecostReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 设置输出key 和 value 的类型

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置输入 输出路径
		FileInputFormat.addInputPath(job, new Path(Constants.JOB_INPUT_PATH));
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		Path outputPath = new Path(Constants.JOB_OUTPUT_PATH);
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);

		//
		job.waitForCompletion(true);

	}

}
