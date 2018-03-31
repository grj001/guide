package com.grj.util;

public class Constants {

	public static final String CRONSCHEDULE = "0/20 * * * * ?";
	
	public static final String MASTER_HOST = "192.168.197.131";
	public static final String MASTER_USERNAME = "root";
	public static final String MASTER_PASSWORD = "root";
	public static final String MASTER_SHELL_SCRIPT = "cd /test/guide/;./guide_sqoop.sh start";
	//bash /test/guide/guide_sqoop.sh
	public static final String MASTER_HDFS_HOST_PORT = "hdfs://192.168.197.131:9000";
	
	
	
	
	
	public static final String JOB_GETRECOST_NAME="getcost";
	public static final String JOB_GETRECOST_INPUT_PATH=MASTER_HDFS_HOST_PORT+"/guide/tmp";
	public static final String JOB_GETRECOST_OUTPUT_PATH=MASTER_HDFS_HOST_PORT+"/guide/out";
	
	public static final String JOB_MERGE_NAME="merge";
	public static final String JOB_MERGE_INPUT_PATH=MASTER_HDFS_HOST_PORT+"/guide/out";
	public static final String JOB_MERGE_OUTPUT_PATH=MASTER_HDFS_HOST_PORT+"/guide/merge";
	
	
	
	//hbase 常量设置
	public static final String HBASE_TABLE_NAME="guide";
	public static final String HBASE_COLUMN_FAMILY_NAME="cf";
	public static final String HBASE_COLUMN_HCOST_NAME="hcost";
	public static final String HBASE_COLUMN_HCOUNT_NAME = "hcount";
	public static final String HBASE_COLUMN_OCOST_NAME = "ocost";
	public static final String HBASE_COLUMN_OCOUNT_NAME = "ocount";
	public static final String MERGE_JOB_OUTPUT_PATH = "hdfs://192.168.6.250:9000/guide/merge";
	
	
}
