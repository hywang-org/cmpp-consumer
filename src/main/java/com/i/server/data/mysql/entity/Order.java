package com.i.server.data.mysql.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_order")
public class Order {
	/**
	 * order_id
	 */
	private String id;

	/**
	 * 运营商id
	 */
	private String spMsgId;

	private String ownMsgId;

	private String protocol;

	/**
	 * 状态，0：发送中 1：成功 2：失败
	 */
	private Integer sendStatus;

	/**
	 * 收信人手机号
	 */
	private String desId;

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
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	public String getId() {
		return id;
	}

	/**
	 * order_id
	 * 
	 * @param id
	 *            order_id
	 */
	public void setId(String id) {
		this.id = id == null ? null : id.trim();
	}

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

	@Column(name = "protocol")
	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	/**
	 * 状态，0：发送中 1：成功 2：失败
	 * 
	 * @return send_status 状态，0：发送中 1：成功 2：失败
	 */
	@Column(name = "send_status")
	public Integer getSendStatus() {
		return sendStatus;
	}

	/**
	 * 状态，0：发送中 1：成功 2：失败
	 * 
	 * @param sendStatus
	 *            状态，0：发送中 1：成功 2：失败
	 */
	public void setSendStatus(Integer sendStatus) {
		this.sendStatus = sendStatus;
	}

	/**
	 * 收信人手机号
	 * 
	 * @return des_id 收信人手机号
	 */
	@Column(name = "des_id")
	public String getDesId() {
		return desId;
	}

	/**
	 * 收信人手机号
	 * 
	 * @param desId
	 *            收信人手机号
	 */
	public void setDesId(String desId) {
		this.desId = desId == null ? null : desId.trim();
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