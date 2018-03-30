package com.grj.util;

public class Constants {

	
	public static final String JOB_NAME="getcost";
	public static final String JOB_INPUT_PATH="hdfs://192.168.6.250:9000/guide/tmp";
	public static final String JOB_OUTPUT_PATH="hdfs://192.168.6.250:9000/guide/out";
	
	//hbase 常量设置
	public static final String HBASE_TABLE_NAME="guide";
	public static final String HBASE_COLUMN_FAMILY_NAME="cf";
	public static final String HBASE_COLUMN_HCOST_NAME="hcost";
	public static final String HBASE_COLUMN_HCOUNT_NAME = "hcount";
	public static final String HBASE_COLUMN_OCOST_NAME = "ocost";
	public static final String HBASE_COLUMN_OCOUNT_NAME = "ocount";
	public static final String MERGE_JOB_OUTPUT_PATH = "hdfs://192.168.6.250:9000/guide/merge";
	
	
}
