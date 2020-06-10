package com.waiter.sparkstreaming.entity;


import java.io.Serializable;
import java.util.Date;

/**
 * @ClassName Stat
 * @Description TOOD
 * @Author Waiter
 * @Date 2020/5/28 22:41
 * @Version 1.0
 */
public class Stat implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;

    private Date createDatetime;

    private Integer pv;

    private Integer uv;

    private Integer ios;

    private Integer android;

    private Integer unknowDevice;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getPv() {
        return pv;
    }

    public void setPv(Integer pv) {
        this.pv = pv;
    }

    public Integer getUv() {
        return uv;
    }

    public void setUv(Integer uv) {
        this.uv = uv;
    }

    public Integer getIos() {
        return ios;
    }

    public void setIos(Integer ios) {
        this.ios = ios;
    }

    public Integer getAndroid() {
        return android;
    }

    public void setAndroid(Integer android) {
        this.android = android;
    }

    public Integer getUnknowDevice() {
        return unknowDevice;
    }

    public void setUnknowDevice(Integer unknowDevice) {
        this.unknowDevice = unknowDevice;
    }

    public Date getCreateDatetime() {
        return createDatetime;
    }

    public void setCreateDatetime(Date createDatetime) {
        this.createDatetime = createDatetime;
    }
}
