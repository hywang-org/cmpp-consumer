package com.i.server.dao.redis;

import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.PostConstruct;

//this redis store supplier's msgid and our appid
//@Repository
public class MsgIdAppIdRedis extends RedisOperationSets {
	//	@Resource(name = "msgIdAppIdRedisDao")
	private RedisTemplate<String, Object> redisTemplate;

	@PostConstruct
	public void Redis1() {
		super.setRedisTemplate(redisTemplate);
	}
}
