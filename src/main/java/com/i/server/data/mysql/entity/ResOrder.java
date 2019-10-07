package com.i.server.data.mysql.entity;

import javax.persistence.*;
import java.util.Date;

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

    private Long id;

    private String ownSeqId;

    private String spMsgId;
    
    private Date shareDate;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long getId() {
        return id;
    }

    /**
     * order_id
     *
     * @param id
     *            order_id
     */
    public void setId(Long id1) {
        this.id = id;
    }

    @Column(name = "own_seq_id")
    public String getOwnSeqId() {
        return ownSeqId;
    }

    public void setOwnSeqId(String ownSeqId) {
        this.ownSeqId = ownSeqId;
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
