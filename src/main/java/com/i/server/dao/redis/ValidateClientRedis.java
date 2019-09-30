package com.i.server.dao.redis;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ValidateClientRedis extends RedisOperationSets {
	@Resource(name = "validateClientRedisDao")
	private RedisTemplate<String, Object> redisTemplate;

	@PostConstruct
	public void Redis1() {
		super.setRedisTemplate(redisTemplate);
	}
}
