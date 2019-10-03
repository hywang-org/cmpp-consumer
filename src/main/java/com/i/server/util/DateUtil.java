package com.i.server.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author 作者 hywang 619201932@qq.com
 *
 * @version 创建时间：2019年9月18日 下午2:41:58
 *
 */
public class DateUtil {

	public static Date LocalDateToUdate() {
		LocalDate localDate = LocalDate.now();
		ZoneId zone = ZoneId.systemDefault();
		Instant instant = localDate.atStartOfDay().atZone(zone).toInstant();
		Date date = Date.from(instant);
		return date;
	}
}
