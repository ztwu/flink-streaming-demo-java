package com.ztwu.bigdata.demo.join;

/**
 * created with idea
 * user:ztwu
 * date:2021/5/16
 * description
 */
public class Entity2 {
    Integer cityId;
    String cityName;
    Long cityTime;

    public Entity2(Integer cityId, String cityName, Long cityTime) {
        this.cityId = cityId;
        this.cityName = cityName;
        this.cityTime = cityTime;
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public Long getCityTime() {
        return cityTime;
    }

    public void setCityTime(Long cityTime) {
        this.cityTime = cityTime;
    }

    @Override
    public String toString() {
        return "Entity2{" +
                "cityId=" + cityId +
                ", cityName='" + cityName + '\'' +
                ", cityTime=" + cityTime +
                '}';
    }
}
