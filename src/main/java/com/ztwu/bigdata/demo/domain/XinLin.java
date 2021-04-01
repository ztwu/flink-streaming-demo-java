package com.ztwu.bigdata.demo.domain;

public class XinLin {

    /**
     * '手机号码
     */
    private String num;

    private String s27;

    private String s28;

    private String s29;

    private String begintime;

    /**
     * 经度
     */
    private String lon;

    /**
     * 纬度
     */
    private String lat;

    /**
     * 处理事件
     */
    private String hand_time;

    /**
     * 基站唯一ID
     */
    private String enbuniqueid;

    /**
     * 基站ID
     */
    private String enb_id;

    /**
     * 区县
     */
    private String region_name;

    /**
     * 信令话单id
     */
    private String xdr_id;

    private String xdr_type;

    /**
     * imsi号
     */
    private String imsi;

    /**
     * imei
     */
    private String imei;

    /**
     * 运营商编码
     */
    private String sp_code;

    /**
     * 网络类型
     */
    private String as_code;

    /**
     * 无线接入技术
     */
    private String rat_type;

    /**
     * 流程类型
     */
    private String procedure_type;

    /**
     * 流程开始时间
     */
    private String begin_time;

    /**
     * 流程结束时间
     */
    private String end_time;

    /**
     * 流程状态
     */
    private String procedure_status;

    /**
     * 归属地编码
     */
    private String home_code;

    /**
     * 区域码
     */
    private String area_code;

    /**
     * 小区id
     */
    private String cell_id;

    /**
     * 包括ecgi/cgi/bsid
     */
    private String ecgi;

    /**
     * 局点id
     */
    private String house_id;

    /**
     * 加载时间
     */
    private String load_time;

    /**
     * source_type
     */
    private String source_type;

    public static XinLin build(String value) {
        String[] values = value.split(",");
        XinLin item = new XinLin();
        int i = 0;
        item.num = values[i++];
        item.s27 = values[i++];
        item.s28 = values[i++];
        item.s29 = values[i++];
        item.begin_time = values[i++];
        item.lon = values[i++];
        item.lat = values[i++];
        item.hand_time = values[i++];
        item.enbuniqueid = values[i++];
        item.enb_id = values[i++];
        item.region_name = values[i++];
        item.xdr_id = values[i++];
        item.xdr_type = values[i++];
        item.imsi = values[i++];
        item.imei = values[i++];
        item.sp_code = values[i++];
        item.as_code = values[i++];
        item.rat_type = values[i++];
        item.procedure_type = values[i++];
        item.begin_time = values[i++];
        item.end_time = values[i++];
        item.procedure_status = values[i++];
        item.home_code = values[i++];
        item.area_code = values[i++];
        item.cell_id = values[i++];
        item.ecgi = values[i++];
        item.house_id = values[i++];
        item.load_time = values[i++];
        item.source_type = values[i++];
        return item;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getS27() {
        return s27;
    }

    public void setS27(String s27) {
        this.s27 = s27;
    }

    public String getS28() {
        return s28;
    }

    public void setS28(String s28) {
        this.s28 = s28;
    }

    public String getS29() {
        return s29;
    }

    public void setS29(String s29) {
        this.s29 = s29;
    }

    public String getBegintime() {
        return begintime;
    }

    public void setBegintime(String begintime) {
        this.begintime = begintime;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getHand_time() {
        return hand_time;
    }

    public void setHand_time(String hand_time) {
        this.hand_time = hand_time;
    }

    public String getEnbuniqueid() {
        return enbuniqueid;
    }

    public void setEnbuniqueid(String enbuniqueid) {
        this.enbuniqueid = enbuniqueid;
    }

    public String getEnb_id() {
        return enb_id;
    }

    public void setEnb_id(String enb_id) {
        this.enb_id = enb_id;
    }

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public String getXdr_id() {
        return xdr_id;
    }

    public void setXdr_id(String xdr_id) {
        this.xdr_id = xdr_id;
    }

    public String getXdr_type() {
        return xdr_type;
    }

    public void setXdr_type(String xdr_type) {
        this.xdr_type = xdr_type;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getSp_code() {
        return sp_code;
    }

    public void setSp_code(String sp_code) {
        this.sp_code = sp_code;
    }

    public String getAs_code() {
        return as_code;
    }

    public void setAs_code(String as_code) {
        this.as_code = as_code;
    }

    public String getRat_type() {
        return rat_type;
    }

    public void setRat_type(String rat_type) {
        this.rat_type = rat_type;
    }

    public String getProcedure_type() {
        return procedure_type;
    }

    public void setProcedure_type(String procedure_type) {
        this.procedure_type = procedure_type;
    }

    public String getBegin_time() {
        return begin_time;
    }

    public void setBegin_time(String begin_time) {
        this.begin_time = begin_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public String getProcedure_status() {
        return procedure_status;
    }

    public void setProcedure_status(String procedure_status) {
        this.procedure_status = procedure_status;
    }

    public String getHome_code() {
        return home_code;
    }

    public void setHome_code(String home_code) {
        this.home_code = home_code;
    }

    public String getArea_code() {
        return area_code;
    }

    public void setArea_code(String area_code) {
        this.area_code = area_code;
    }

    public String getCell_id() {
        return cell_id;
    }

    public void setCell_id(String cell_id) {
        this.cell_id = cell_id;
    }

    public String getEcgi() {
        return ecgi;
    }

    public void setEcgi(String ecgi) {
        this.ecgi = ecgi;
    }

    public String getHouse_id() {
        return house_id;
    }

    public void setHouse_id(String house_id) {
        this.house_id = house_id;
    }

    public String getLoad_time() {
        return load_time;
    }

    public void setLoad_time(String load_time) {
        this.load_time = load_time;
    }

    public String getSource_type() {
        return source_type;
    }

    public void setSource_type(String source_type) {
        this.source_type = source_type;
    }
}
