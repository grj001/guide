package com.grj.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class HbaseUtil {

	public static Connection getConnection() {
		
		Configuration configuration = HBaseConfiguration.create();

		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Result getData(
			String rowkey, String hbaseTableName,Connection connection) 
			throws IOException {
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		Get get = new Get(rowkey.getBytes());
		return table.get(get);
	}

	public static void savePut(
			Put put, String hbaseTableName,Connection connection) 
					throws IOException {
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		table.put(put);
		
	}

}
