package com.grj.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetRecostMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 输入的是两个文件 先判断两个不同数据
		String info[] = value.toString().split("\t");
		
		// 读取的是报销信息 获取hostipalId
		String hospitalId = info[0];
		context.write(new Text(hospitalId), value);

	

	}

}
