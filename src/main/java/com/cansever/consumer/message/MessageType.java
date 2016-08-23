package com.cansever.consumer.message;

/**
 * User: TTACANSEVER
 */
public enum MessageType {
    TEXT ("T"),
    BUZZ ("B"),
    PHOTO ("P"),
    IMAGE ("P"),
    VIDEO ("V"),
    AUDIO ("A"),
    LOCATION ("L"),
    CONTACT("C"),
    STICKER ("S"),
    GIF ("G"),
    CAPS_MEME ("M"),
    CAPS ("M"),
    REGISTER ("R"),
    MUCROOM_JOIN ("J"),
    MESSAGE_WITHOUT_PREVIEW ("NP"),
    SECRET_MESSAGE ("E"),
    INCOMING_CALL ("IC"),
    MISSED_CALL ("MC");

    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    private MessageType(String key) {
        this.key = key;
    }

}