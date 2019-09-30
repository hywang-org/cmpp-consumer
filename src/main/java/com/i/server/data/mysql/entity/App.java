package com.i.server.data.mysql.entity;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_app")
public class App {
	/**
	 * 主键自增Id
	 */
	private Long id;

	/**
	 * 接入号
	 */
	private String appId;

	/**
	 * 产品名称
	 */
	private String appName;

	/**
	 * 用户Id
	 */
	private String userId;

	/**
	 * 所属领域
	 */
	private String field;

	/**
	 * 协议类型
	 */
	private int protocolType;

	/**
	 * 回调地址
	 */
	private String callbackUrl;

	/**
	 * 状态，0：启用 1：禁用
	 */
	private Integer appStatus;

	/**
	 * 扩展码
	 */
	private String extendCode;

	/**
	 * 流速
	 */
	private Integer speedLimit;

	/**
	 * 开始发送时间
	 */
	private Date sendBeginTime;

	/**
	 * 结束发送时间
	 */
	private Date sendEndTime;

	/**
	 * 单条短信价格
	 */
	private BigDecimal price;

	/**
	 * 支付类型 0 为预付款 1为后付款
	 */
	private Integer payType;

	/**
	 * 通道
	 */
	private String channel;

	/**
	 * 创建时间
	 */
	private Date createdDate;

	/**
	 * 更新时间
	 */
	private Date updatedDate;

	/**
	 * 主键自增Id
	 * 
	 * @return id 主键自增Id
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	public Long getId() {
		return id;
	}

	/**
	 * 主键自增Id
	 * 
	 * @param id
	 *            主键自增Id
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * 接入号
	 * 
	 * @return app_id 接入号
	 */
	@Column(name = "app_id")
	public String getAppId() {
		return appId;
	}

	/**
	 * 接入号
	 * 
	 * @param appId
	 *            接入号
	 */
	public void setAppId(String appId) {
		this.appId = appId == null ? null : appId.trim();
	}

	/**
	 * 产品名称
	 * 
	 * @return app_name 产品名称
	 */
	@Column(name = "app_name")
	public String getAppName() {
		return appName;
	}

	/**
	 * 产品名称
	 * 
	 * @param appName
	 *            产品名称
	 */
	public void setAppName(String appName) {
		this.appName = appName == null ? null : appName.trim();
	}

	/**
	 * 用户Id
	 * 
	 * @return user_id 用户Id
	 */
	@Column(name = "user_id")
	public String getUserId() {
		return userId;
	}

	/**
	 * 用户Id
	 * 
	 * @param userId
	 *            用户Id
	 */
	public void setUserId(String userId) {
		this.userId = userId == null ? null : userId.trim();
	}

	/**
	 * 所属领域
	 * 
	 * @return field 所属领域
	 */
	@Column(name = "field")
	public String getField() {
		return field;
	}

	/**
	 * 所属领域
	 * 
	 * @param field
	 *            所属领域
	 */
	public void setField(String field) {
		this.field = field == null ? null : field.trim();
	}

	@Column(name = "protocol_type")
	public int getProtocolType() {
		return protocolType;
	}

	public void setProtocolType(int protocolType) {
		this.protocolType = protocolType;
	}

	/**
	 * 回调地址
	 * 
	 * @return callback_url 回调地址
	 */
	@Column(name = "callback_url")
	public String getCallbackUrl() {
		return callbackUrl;
	}

	/**
	 * 回调地址
	 * 
	 * @param callbackUrl
	 *            回调地址
	 */
	public void setCallbackUrl(String callbackUrl) {
		this.callbackUrl = callbackUrl == null ? null : callbackUrl.trim();
	}

	/**
	 * 状态，0：启用 1：禁用
	 * 
	 * @return app_status 状态，0：启用 1：禁用
	 */
	@Column(name = "app_status")
	public Integer getAppStatus() {
		return appStatus;
	}

	/**
	 * 状态，0：启用 1：禁用
	 * 
	 * @param appStatus
	 *            状态，0：启用 1：禁用
	 */
	public void setAppStatus(Integer appStatus) {
		this.appStatus = appStatus;
	}

	/**
	 * 扩展码
	 * 
	 * @return extendCode 扩展码
	 */
	@Column(name = "extend_code")
	public String getExtendcode() {
		return extendCode;
	}

	/**
	 * 扩展码
	 * 
	 * @param extendcode
	 *            扩展码
	 */
	public void setExtendcode(String extendCode) {
		this.extendCode = extendCode == null ? null : extendCode.trim();
	}

	/**
	 * 流速
	 * 
	 * @return speed_limit 流速
	 */
	@Column(name = "speed_limit")
	public Integer getSpeedLimit() {
		return speedLimit;
	}

	/**
	 * 流速
	 * 
	 * @param speedLimit
	 *            流速
	 */
	public void setSpeedLimit(Integer speedLimit) {
		this.speedLimit = speedLimit;
	}

	/**
	 * 开始发送时间
	 * 
	 * @return send_begin_time 开始发送时间
	 */
	@Column(name = "send_begin_time")
	public Date getSendBeginTime() {
		return sendBeginTime;
	}

	/**
	 * 开始发送时间
	 * 
	 * @param sendBeginTime
	 *            开始发送时间
	 */
	public void setSendBeginTime(Date sendBeginTime) {
		this.sendBeginTime = sendBeginTime;
	}

	/**
	 * 结束发送时间
	 * 
	 * @return send_end_time 结束发送时间
	 */
	@Column(name = "send_end_time")
	public Date getSendEndTime() {
		return sendEndTime;
	}

	/**
	 * 结束发送时间
	 * 
	 * @param sendEndTime
	 *            结束发送时间
	 */
	public void setSendEndTime(Date sendEndTime) {
		this.sendEndTime = sendEndTime;
	}

	/**
	 * 单条短信价格
	 * 
	 * @return price 单条短信价格
	 */
	@Column(name = "price")
	public BigDecimal getPrice() {
		return price;
	}

	/**
	 * 单条短信价格
	 * 
	 * @param price
	 *            单条短信价格
	 */
	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	/**
	 * 支付类型 0 为预付款 1为后付款
	 * 
	 * @return pay_type 支付类型 0 为预付款 1为后付款
	 */
	@Column(name = "pay_type")
	public Integer getPayType() {
		return payType;
	}

	/**
	 * 支付类型 0 为预付款 1为后付款
	 * 
	 * @param payType
	 *            支付类型 0 为预付款 1为后付款
	 */
	public void setPayType(Integer payType) {
		this.payType = payType;
	}

	/**
	 * 通道
	 * 
	 * @return channel 通道
	 */
	@Column(name = "channel")
	public String getChannel() {
		return channel;
	}

	/**
	 * 通道
	 * 
	 * @param channel
	 *            通道
	 */
	public void setChannel(String channel) {
		this.channel = channel == null ? null : channel.trim();
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