package com.i.server.data.mysql.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_user_info")
public class UserInfo {
	/**
	 * 主键自增Id
	 */
	private Long id;

	/**
	 * 用户Id
	 */
	private String userId;

	/**
	 * 用户名称
	 */
	private String userName;

	/**
	 * 地址
	 */
	private String address;

	/**
	 * 对接人
	 */
	private String contacter;

	/**
	 * 付费类型
	 */
	private Integer payType;

	/**
	 * 账户状态，0：启用 1：禁用
	 */
	private Integer userStatus;

	/**
	 * 用户手机号
	 */
	private String mobile;

	/**
	 * 用户邮箱
	 */
	private String email;

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
	 * 用户名称
	 * 
	 * @return user_name 用户名称
	 */
	@Column(name = "user_name")
	public String getUserName() {
		return userName;
	}

	/**
	 * 用户名称
	 * 
	 * @param userName
	 *            用户名称
	 */
	public void setUserName(String userName) {
		this.userName = userName == null ? null : userName.trim();
	}

	/**
	 * 地址
	 * 
	 * @return address 地址
	 */
	@Column(name = "address")
	public String getAddress() {
		return address;
	}

	/**
	 * 地址
	 * 
	 * @param address
	 *            地址
	 */
	public void setAddress(String address) {
		this.address = address == null ? null : address.trim();
	}

	/**
	 * 对接人
	 * 
	 * @return contacter 对接人
	 */
	@Column(name = "contacter")
	public String getContacter() {
		return contacter;
	}

	/**
	 * 对接人
	 * 
	 * @param contacter
	 *            对接人
	 */
	public void setContacter(String contacter) {
		this.contacter = contacter == null ? null : contacter.trim();
	}

	@Column(name = "pay_type")
	public Integer getPayType() {
		return payType;
	}

	public void setPayType(Integer payType) {
		this.payType = payType;
	}

	/**
	 * 账户状态，0：启用 1：禁用
	 * 
	 * @return user_status 账户状态，0：启用 1：禁用
	 */
	@Column(name = "user_status")
	public Integer getUserStatus() {
		return userStatus;
	}

	/**
	 * 账户状态，0：启用 1：禁用
	 * 
	 * @param userStatus
	 *            账户状态，0：启用 1：禁用
	 */
	public void setUserStatus(Integer userStatus) {
		this.userStatus = userStatus;
	}

	/**
	 * 用户手机号
	 * 
	 * @return mobile 用户手机号
	 */
	@Column(name = "mobile")
	public String getMobile() {
		return mobile;
	}

	/**
	 * 用户手机号
	 * 
	 * @param mobile
	 *            用户手机号
	 */
	public void setMobile(String mobile) {
		this.mobile = mobile == null ? null : mobile.trim();
	}

	/**
	 * 用户邮箱
	 * 
	 * @return email 用户邮箱
	 */
	@Column(name = "email")
	public String getEmail() {
		return email;
	}

	/**
	 * 用户邮箱
	 * 
	 * @param email
	 *            用户邮箱
	 */
	public void setEmail(String email) {
		this.email = email == null ? null : email.trim();
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