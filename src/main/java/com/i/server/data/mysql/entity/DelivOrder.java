package com.i.server.data.mysql.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author 作者 :hywang
 *
 * @version 创建时间：2019年9月12日 下午4:24:49
 *
 * @version 1.0
 */
@Entity
@Table(name = "tbl_deliv_order")
public class DelivOrder {

    private String id;

    private String spMsgId;

    private String ownMsgId;

    private String appId;

    private String msgState;
    
    private Date shareDate;

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

    @Column(name = "app_id")
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId == null ? null : appId.trim();
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

    @Column(name = "msg_state")
    public String getMsgState() {
        return msgState;
    }

    public void setMsgState(String msgState) {
        this.msgState = msgState;
    }

    @Column(name = "share_date")
	public Date getShareDate() {
		return shareDate;
	}

	public void setShareDate(Date shareDate) {
		this.shareDate = shareDate;
	}
}
