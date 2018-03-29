package com.grj.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.grj.StartMain;
import com.grj.util.Constants;
import com.grj.util.HbaseUtil;

public class MergeReducer extends Reducer<Text, Text, NullWritable, Text> {

	public static Logger logger = LoggerFactory.getLogger(MergeReducer.class);
	
	public static Connection connection = HbaseUtil.getConnection();

	@SuppressWarnings("deprecation")
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		/// 000e6a9e-f80e-4ed0-82a8-5ab36fc087ef PDY00083X41010217A5152 Q70.100
		/// 3201 dbac8fb4-58cb-4320-a0bb-b88a520781a9 2 2014-04-14 2014-04-14
		/// 592.52 0 2014-04-14 592.28

		// 为了简化 我们只计算 人均住院花费，人均门诊花费 住院总费用 门诊总费用 住院总人数 门诊总人数
		int hcount = 0;
		int ocount = 0;
		double ocost = 0.0;
		double hcost = 0.0;
		for (Text value : values) {
			String[] info = value.toString().split("\t");
			// 拿到flag flag=1 住院 flag=2门诊
			String flag = info[5];
			if (flag.equals("2")) {
				// 处理门诊的信息
				ocount++;
				ocost += Double.valueOf(info[8]);
			} else {
				hcount++;
				hcost += Double.valueOf(info[8]);
			}
		}
		// 从hbase中拿到历史数据 然后合并
		Result result = getDataFromHbase(key, connection);
		// HashMap<String, Object> hashMap = new HashMap<>();

		MergeDataAndSave(key, hcount, ocount, hcost, ocost, result, connection);

		// HbaseUtil.getConnection();

	}

	private void MergeDataAndSave(Text key, int hcount, int ocount, double hcost, double ocost, Result result,
			Connection connection) throws IOException {
		Cell hcostkeyValue = null;
		Cell hcountkeyValue = null;
		Cell ocostkeyValue = null;
		Cell ocountkeyValue = null;

			hcostkeyValue = result.getColumnLatestCell(Constants.HBASE_COLUMN_FAMILY_NAME.getBytes(),
					Constants.HBASE_COLUMN_HCOST_NAME.getBytes());
			hcountkeyValue = result.getColumnLatestCell(Constants.HBASE_COLUMN_FAMILY_NAME.getBytes(),
					Constants.HBASE_COLUMN_HCOUNT_NAME.getBytes());
			ocostkeyValue = result.getColumnLatestCell(Constants.HBASE_COLUMN_FAMILY_NAME.getBytes(),
					Constants.HBASE_COLUMN_OCOST_NAME.getBytes());
			ocountkeyValue = result.getColumnLatestCell(Constants.HBASE_COLUMN_FAMILY_NAME.getBytes(),
					Constants.HBASE_COLUMN_OCOUNT_NAME.getBytes());


			
			double newhcost = 0.0;
			double newocost = 0.0;
			int newhcount = 0;
			int newocount = 0;	
		// 如果历史数据不存在，则keyvalue为空 为此需要三元运算符 当keyvalue为空的时候 默认的值为0
		/*if(hcostkeyValue != null){
			logger.info("*******************"+hcostkeyValue);
			newhcost = hcost + Bytes.toDouble(Bytes.toBytes(0.0));
		}	*/
		
		if(hcostkeyValue != null){
			newhcost = hcost + Bytes.toDouble(CellUtil.cloneValue(hcostkeyValue));
		}else{
			newhcost = hcost + Bytes.toDouble(Bytes.toBytes(0D));
		}
		
		if(ocostkeyValue != null){
			newocost = hcost + Bytes.toDouble(CellUtil.cloneValue(ocostkeyValue));
		}else{
			newocost = hcost + Bytes.toDouble(Bytes.toBytes(0D));
		}
		
		if(hcountkeyValue != null){
			newhcount = hcount + Bytes.toInt(CellUtil.cloneValue(hcountkeyValue));
		}else{
			newhcount = hcount + Bytes.toInt(Bytes.toBytes(0D));
		}
		
		if(ocountkeyValue != null){
			newocount = ocount + Bytes.toInt(CellUtil.cloneValue(ocountkeyValue));
		}else{
			newocount = ocount + Bytes.toInt(Bytes.toBytes(0D));
		}
		

		
		
		// 把新的中间结果保存到hbase
		Put put = new Put(key.toString().getBytes());

		put.addColumn(Constants.HBASE_COLUMN_FAMILY_NAME.getBytes(), Constants.HBASE_COLUMN_HCOST_NAME.getBytes(),
				Bytes.toBytes(newhcost));

		HbaseUtil.savePut(put, Constants.HBASE_TABLE_NAME, connection);

	}

	private Result getDataFromHbase(Text key, Connection connection) throws IOException {
		return HbaseUtil.getData(key.toString(), Constants.HBASE_TABLE_NAME, connection);

	}

	@Override
	protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		connection.close();
	}

}
