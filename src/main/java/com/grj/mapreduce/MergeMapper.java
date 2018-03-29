package com.grj.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * 进行 当前数据 和历史数据的合并  ，合并后将中间结果再保存在hbase中
 */
public class MergeMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		///000e6a9e-f80e-4ed0-82a8-5ab36fc087ef	PDY00083X41010217A5152	Q70.100	3201	dbac8fb4-58cb-4320-a0bb-b88a520781a9	2	2014-04-14	2014-04-14	592.52	0	2014-04-14	592.28
		String[] info = value.toString().split("\t");
		String hospitalId=info[1];
		//从hbase中拿到历史数据  然后合并  
		//为了简化  我们只计算  人均住院花费，人均门诊花费  住院总费用 门诊总费用 住院总人数 门诊总人数 
		context.write(new Text(hospitalId), value);
			
	

	}

}
