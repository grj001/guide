package com.grj.util;

import java.util.HashMap;

import org.springframework.data.redis.core.RedisTemplate;

import com.grj.GuideJob;


public class RedisUtil {
	

	public static void save(String rowkey,HashMap<String, String> info) {
		
		RedisTemplate<String, ?> redisTemplate = GuideJob.applicationContext.getBean(RedisTemplate.class);
		//redisTemplate.opsForValue().set(rowkey, info);
		redisTemplate.opsForHash().putAll(rowkey, info);
	}

}
