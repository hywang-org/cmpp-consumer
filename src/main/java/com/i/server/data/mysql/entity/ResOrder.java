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
 * @version 创建时间：2019年9月12日 下午4:21:42
 *
 * @version 1.0
 */
@Entity
@Table(name = "tbl_res_order")
public class ResOrder {

    private String id;

    private String mySeqId;

    private String spMsgId;
    
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

    @Column(name = "my_seq_id")
    public String getMySeqId() {
        return mySeqId;
    }

    public void setMySeqId(String mySeqId) {
        this.mySeqId = mySeqId;
    }

    @Column(name = "sp_msg_id")
    public String getSpMsgId() {
        return spMsgId;
    }

    public void setSpMsgId(String spMsgId) {
        this.spMsgId = spMsgId;
    }
    
    @Column(name = "share_date")
	public Date getShareDate() {
		return shareDate;
	}

	public void setShareDate(Date shareDate) {
		this.shareDate = shareDate;
	}
}
