package com.i.server.data.mysql.dao;

import java.io.Serializable;
import java.util.List;

public interface Dao {

	public <T> void save(T entity);

	public <T> void update(T entity);

	public void delete(Object entity);

	public int bulkUpdate(String hql, Object... values);

	public int bulkUpdateBySQL(final String sql, final Object... values);

	public <T> T get(Class<T> clazz, Serializable id);

	public <T> List<T> find(String hql, Object... values);

	public <T> T findSingle(String hql, Object... values);

	public <T> List<T> find(String hql, String[] paramNames, Object[] params);

	public <T> List<T> findBySQL(String sql, Object... values);

	public <T> T findSingleBySQL(String sql, Object... values);

	public <T> List<T> findBySQL(String sql, String[] paramNames, Object[] params);

	public void doInTx(Tx tx);

	public static interface Tx {

		void exec();

	}

}
