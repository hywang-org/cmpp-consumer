package com.i.server.rabbitmq.service;

import com.alibaba.fastjson.JSONObject;

public class JsonConvertUtils {
	public static MqEntity convertJSONToObject(JSONObject json) {
		return JSONObject.toJavaObject(json, MqEntity.class);
	}
}
