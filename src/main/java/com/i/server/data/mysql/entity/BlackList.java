package com.i.server.data.mysql.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;

import org.springframework.data.annotation.Id;

import com.sleepycat.persist.model.Entity;

@Entity
@Table(name = "tbl_black_list")
public class BlackList {
	/**
	 * 主键自增Id
	 */
	private Long id;

	/**
	 * app_id
	 */
	private String appId;

	/**
	 * 电话号码
	 */
	private String mobile;

	/**
	 * 类别
	 */
	private Integer type;

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
	 * 电话号码
	 * 
	 * @return mobile 电话号码
	 */
	@Column(name = "mobile")
	public String getMobile() {
		return mobile;
	}

	/**
	 * 电话号码
	 * 
	 * @param mobile
	 *            电话号码
	 */
	public void setMobile(String mobile) {
		this.mobile = mobile == null ? null : mobile.trim();
	}

	/**
	 * 类别
	 * 
	 * @return type 类别
	 */
	@Column(name = "type")
	public Integer getType() {
		return type;
	}

	/**
	 * 类别
	 * 
	 * @param type
	 *            类别
	 */
	public void setType(Integer type) {
		this.type = type;
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