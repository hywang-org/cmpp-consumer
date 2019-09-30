package com.i.server.data.mysql.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_order_result")
public class OrderResult {

	/**
	 * 运营商id
	 */
	private String spMsgId;

	private String ownMsgId;

	/**
	 * app_id
	 */
	private String appId;

	/**
	 * 创建时间
	 */
	private Date createdDate;

	/**
	 * 更新时间
	 */
	private Date updatedDate;

	/**
	 * order_id
	 * 
	 * @return id order_id
	 */
	@Id
	@Column(name = "sp_msg_id")
	public String getSpMsgId() {
		return spMsgId;
	}

	public void setSpMsgId(String spMsgId) {
		this.spMsgId = spMsgId;
	}

	@Column(name = "own_msg_id")
	public String getOwnMsgId() {
		return ownMsgId;
	}

	public void setOwnMsgId(String ownMsgId) {
		this.ownMsgId = ownMsgId;
	}

	/**
	 * app_id
	 * 
	 * @return app_id app_id
	 */
	@Column(name = "app_id")
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

	/**
	 * 创建时间
	 * 
	 * @return created_date 创建时间
	 */
	@Column(name = "created_date")
	public Date getCreatedDate() {
		return createdDate;
	}

	/**
	 * 创建时间
	 * 
	 * @param createdDate
	 *            创建时间
	 */
	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}

	/**
	 * 更新时间
	 * 
	 * @return updated_date 更新时间
	 */
	@Column(name = "updated_date")
	public Date getUpdatedDate() {
		return updatedDate;
	}

	/**
	 * 更新时间
	 * 
	 * @param updatedDate
	 *            更新时间
	 */
	public void setUpdatedDate(Date updatedDate) {
		this.updatedDate = updatedDate;
	}
}