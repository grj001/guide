package com.grj.util;

import java.util.HashMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.redis.core.RedisTemplate;

import com.grj.StartMain;


public class RedisUtil {
	

	public static void save(String rowkey,HashMap<String, String> info) {
		
		RedisTemplate redisTemplate = StartMain.applicationContext.getBean(RedisTemplate.class);
		//redisTemplate.opsForValue().set(rowkey, info);
		redisTemplate.opsForHash().putAll(rowkey, info);
	}

}
