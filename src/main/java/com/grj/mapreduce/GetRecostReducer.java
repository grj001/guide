package com.grj.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetRecostReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	public static Logger logger = LoggerFactory.getLogger(GetRecostReducer.class);
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String record = null;
		String reimbursetime = null;
		double sum=0.0;
		for (Text value : values) {
			//value 两种类型  一中record 一中reimburse 按照长度不同区分  如果是record 则取record全部信息  如果是reimburse 则求和
			String[] info = value.toString().split("\t");
			
			if (info.length==3) {
				sum+=Double.valueOf(info[2]);
				reimbursetime=info[1];
			}else{
				record=value.toString();
			}
			
		}
		String valueOut=record+"\t"+reimbursetime+"\t"+sum;
		
		context.write(NullWritable.get(), new Text(valueOut));
		
	}

}
