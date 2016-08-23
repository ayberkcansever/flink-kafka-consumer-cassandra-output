package com.cansever.consumer.message;

/**
 * User: TTACANSEVER
 */
public class MessageObject {

    private String msgId;
    private String username;
    private String jid;
    private String stanza;
    private long sentTime;

    public MessageObject(String msgId, String username, String jid, String stanza, long sentTime) {
        this.msgId = msgId;
        this.username = username;
        this.jid = jid;
        this.stanza = stanza;
        this.sentTime = sentTime;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getJid() {
        return jid;
    }

    public void setJid(String jid) {
        this.jid = jid;
    }

    public String getStanza() {
        return stanza;
    }

    public void setStanza(String stanza) {
        this.stanza = stanza;
    }

    public long getSentTime() {
        return sentTime;
    }

    public void setSentTime(long sentTime) {
        this.sentTime = sentTime;
    }
}
