package com.i.server.dao.redis;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

@Repository
public abstract class RedisOperationSets {

	private RedisTemplate<String, Object> redisTemplate;

	public RedisTemplate<String, Object> getRedisTemplate() {
		return redisTemplate;
	}

	public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	/**
	 * 插入键值
	 * 
	 * @param key
	 * @param value
	 */
	public void set(final String key, final String value) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.opsForValue().set(key, value);
				return null;
			}
		});
	}

	/**
	 * 插入键值，指定过期时间
	 * 
	 * @param key
	 * @param value
	 * @param seconds过期时间
	 *            (单位s)
	 */
	public void set(final String key, final String value, final Long seconds) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.opsForValue().set(key, value, seconds, TimeUnit.SECONDS);
				return null;
			}
		});
	}

	/**
	 * 插入键值，同时指定过期时间
	 * 
	 * @param key
	 * @param value
	 * @param seconds过期时间
	 *            (单位s)
	 * @return 插入是否成功(1成功/0失败)
	 */
	public Boolean setNX(final String key, final String value, final Long seconds) {
		List<Object> result = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().setIfAbsent(key, value);
				operations.expire(key, seconds, TimeUnit.SECONDS);
				return operations.exec();
			}
		});
		return (Boolean) result.get(0);
	}

	/**
	 * 设置键的过期时间
	 * 
	 * @param key
	 * @param seconds
	 * @return
	 */
	public boolean expire(final String key, final Long seconds) {
		Boolean result = redisTemplate.execute(new SessionCallback<Boolean>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Boolean execute(RedisOperations operations) throws DataAccessException {
				return operations.expire(key, seconds, TimeUnit.SECONDS);
			}
		});
		return result;
	}

	/**
	 * 插入键值，同时写入set集合
	 * 
	 * @param setKey
	 *            集合key
	 * @param setValue
	 *            集合value ，同时为键值的key
	 * @param value
	 *            键值value
	 */
	public void setValueAndAddList(final String setKey, final String key, final String value) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key, value);
				operations.opsForSet().add(setKey, key);
				return operations.exec();
			}
		});
	}

	/**
	 * 删除键值，同时从集合zset中删除
	 * 
	 * @param setKey
	 *            集合key
	 * @param key
	 *            集合value ，同时为键值的key
	 */
	public void deleteListKey(final String setKey, final String key) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.delete(key);
				operations.opsForSet().remove(setKey, key);
				return operations.exec();
			}
		});
	}

	/**
	 * 插入键值，同时写入有序zset集合
	 * 
	 * @param zSetKey
	 *            集合key
	 * @param key
	 *            集合value ，同时为键值的key
	 * @param value
	 *            键值value
	 */
	public void setNxAndZAdd(final String zSetKey, final String key, final String value, final long score) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key, value);
				operations.opsForZSet().add(zSetKey, key, score);
				return operations.exec();
			}
		});
	}

	/**
	 * 删除键值，同时从集合zset中删除
	 * 
	 * @param zSetKey
	 *            集合key
	 * @param key
	 *            集合value ，同时为键值的key
	 */
	public void deleteKey(final String zSetKey, final String key) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.delete(key);
				operations.opsForZSet().remove(zSetKey, key);
				return operations.exec();
			}
		});
	}

	/**
	 * 从有序集合中取指定范围的值
	 * 
	 * @param zSetKey
	 *            集合key
	 * @param min
	 *            开始范围
	 * @param max
	 *            结束范围
	 * @return
	 */
	public Set<String> getZRangeByScore(final String zSetKey, final long min, final long max) {
		Set<String> results = redisTemplate.execute(new SessionCallback<Set<String>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Set<String> execute(RedisOperations operations) throws DataAccessException {
				Set<String> scoreSet = operations.opsForZSet().rangeByScore(zSetKey, min, max);
				return scoreSet;
			}
		});
		return results;
	}

	/**
	 * 获取指定key的值
	 * 
	 * @param key
	 * @return
	 */
	public String get(final String key) {
		Object result = redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings("rawtypes")
			public Object execute(RedisOperations operations) throws DataAccessException {
				return operations.opsForValue().get(key);
			}
		});
		return result == null ? null : result.toString();
	}

	/**
	 * 通过集合的key获取集合内容
	 * 
	 * @param setKey
	 * @return
	 */
	public Set<String> getSetValue(final String setKey) {
		Set<String> result = redisTemplate.execute(new SessionCallback<Set<String>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Set<String> execute(RedisOperations operations) throws DataAccessException {
				Set<String> value = operations.opsForSet().members(setKey);
				return value;
			}
		});
		return result;
	}

	/**
	 * 通过指定正则获取redis中key
	 * 
	 * @param pattern
	 *            查询条件 例：pattern=*
	 * @return
	 */
	public Set<String> getKeys(final String pattern) {
		Set<String> result = redisTemplate.execute(new SessionCallback<Set<String>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public Set<String> execute(RedisOperations operations) throws DataAccessException {
				Set<String> value = operations.keys(pattern);
				return value;
			}
		});
		return result;
	}

	public void deleteKey(final String key) {
		redisTemplate.execute(new SessionCallback<Object>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.delete(key);
				// operations.opsForZSet().remove("UserSessionSet", key);
				return operations.exec();
			}
		});
	}

	public long incrAndExpire(final String key, final Long seconds) {
		List<Object> result = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().increment(key, 1);
				operations.expire(key, seconds, TimeUnit.SECONDS);
				return operations.exec();
			}
		});
		return (long) result.get(0);
	}

	public long incr(final String key) {
		List<Object> result = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().increment(key, 1);
				return operations.exec();
			}
		});
		return (long) result.get(0);
	}

	public boolean eval(String script, List<String> keys, Object... args) {
		return redisTemplate.execute(new DefaultRedisScript<>(script, Boolean.class), keys, args);
	}

}
