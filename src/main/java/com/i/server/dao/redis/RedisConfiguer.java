package com.i.server.dao.redis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfiguer {

	@Value("${redis.hostName}")
	private String hostName;

	@Value("${redis.port}")
	private int port;

	@Value("${redis.password}")
	private String password;

	@Value("${redis.maxIdle}")
	private int maxIdle;

	@Value("${redis.maxTotal}")
	private int maxTotal;

	@Value("${redis.timeOutSeconds}")
	private int timeOutSeconds;

	@Value("${redis.maxWaitMillis}")
	private long maxWaitMillis;

	@Value("${redis.database0}")
	private int database0;

	@Value("${redis.database1}")
	private int database1;

	@Value("${redis.database2}")
	private int database2;

	@Value("${redis.database3}")
	private int database3;

	@Value("${redis.database4}")
	private int database4;

	@Bean(name = "validateClientRedisDao")
	public StringRedisTemplate redisTemplateConnectDB2() {
		StringRedisTemplate temple = new StringRedisTemplate();
		temple.setConnectionFactory(
				connectionFactory(hostName, port, password, maxIdle, maxTotal, database0, maxWaitMillis, false));
		return temple;
	}

	@Bean(name = "redisChannelInfo")
	public StringRedisTemplate redisTemplateConnectDB1() {
		StringRedisTemplate temple = new StringRedisTemplate();
		temple.setConnectionFactory(
				connectionFactory(hostName, port, password, maxIdle, maxTotal, database1, maxWaitMillis, false));
		return temple;
	}

//	@Bean(name = "msgIdAppIdRedisDao")
//	public StringRedisTemplate redisTemplateConnectDB() {
//		StringRedisTemplate temple = new StringRedisTemplate();
//		temple.setConnectionFactory(
//				connectionFactory(hostName, port, password, maxIdle, maxTotal, database4, maxWaitMillis, false));
//		return temple;
//	}

	@Bean(name = "redisDaoGetMaxConcurrentTransNum")
	public StringRedisTemplate redisTemplateConnectDB3() {
		StringRedisTemplate temple = new StringRedisTemplate();
		temple.setConnectionFactory(
				connectionFactory(hostName, port, password, maxIdle, maxTotal, database2, maxWaitMillis, false));
		return temple;
	}

	@Bean(name = "redisProducer")
	public StringRedisTemplate redisTemplateConnectDB4() {
		StringRedisTemplate temple = new StringRedisTemplate();
		temple.setConnectionFactory(
				connectionFactory(hostName, port, password, maxIdle, maxTotal, database3, maxWaitMillis, false));
		return temple;
	}

	@Bean(name = "consumerRedisDao")
	public StringRedisTemplate saveDeletedOrders() {
		StringRedisTemplate temple = new StringRedisTemplate();
		temple.setConnectionFactory(
				connectionFactory(hostName, port, password, maxIdle, maxTotal, database4, maxWaitMillis, false));
		return temple;
	}

	public RedisConnectionFactory connectionFactory(String hostName, int port, String password, int maxIdle,
	                                                int maxTotal, int index, long maxWaitMillis, boolean testOnBorrow) {
		JedisConnectionFactory jedis = new JedisConnectionFactory();
		jedis.setHostName(hostName);
		jedis.setPort(port);
		if (!StringUtils.isEmpty(password)) {
			jedis.setPassword(password);
		}
		if (index != 0) {
			jedis.setDatabase(index);
		}
		jedis.setPoolConfig(poolCofig(maxIdle, maxTotal, maxWaitMillis, testOnBorrow));
		jedis.afterPropertiesSet();
		// 初始化连接pool
		return jedis;
	}

	public JedisPoolConfig poolCofig(int maxIdle, int maxTotal, long maxWaitMillis, boolean testOnBorrow) {
		JedisPoolConfig poolCofig = new JedisPoolConfig();
		poolCofig.setMaxIdle(maxIdle);
		poolCofig.setMaxTotal(maxTotal);
		poolCofig.setMaxWaitMillis(maxWaitMillis);
		poolCofig.setTestOnBorrow(testOnBorrow);
		return poolCofig;
	}

}
