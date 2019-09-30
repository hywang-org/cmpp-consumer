package com.i.server.data.mysql.entity;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_pro_secret")
public class ProSecret {
	/**
	 * app_id
	 */
	private String appId;

	/**
	 * app_secret
	 */
	private String appSecret;

	/**
	 * 创建时间
	 */
	private String createdDate;

	/**
	 * 更新时间
	 */
	private String updatedDate;

	/**
	 * app_id
	 * 
	 * @return app_id app_id
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	public String getAppId() {
		return appId;
	}

	/**
	 * app_id
	 * 
	 * @param appId
	 *            app_id
	 */
	public void setAppId(String appId) {
		this.appId = appId == null ? null : appId.trim();
	}

	@Column(name = "app_secret")
	public String getAppSecret() {
		return appSecret;
	}

	public void setAppSecret(String appSecret) {
		this.appSecret = appSecret;
	}

	@Column(name = "created_date")
	public String getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(String createdDate) {
		this.createdDate = createdDate;
	}

	@Column(name = "updated_date")
	public String getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(String updatedDate) {
		this.updatedDate = updatedDate;
	}

}