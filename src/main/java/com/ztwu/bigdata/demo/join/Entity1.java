package com.ztwu.bigdata.demo.join;

/**
 * created with idea
 * user:ztwu
 * date:2021/5/16
 * description
 */
public class Entity1 {
    String userId;
    Integer cityId;
    Long mytime;

    public Entity1(String userId, Integer cityId, Long mytime) {
        this.userId = userId;
        this.cityId = cityId;
        this.mytime = mytime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public Long getMytime() {
        return mytime;
    }

    public void setMytime(Long mytime) {
        this.mytime = mytime;
    }

    @Override
    public String toString() {
        return "Entity1{" +
                "userId='" + userId + '\'' +
                ", cityId=" + cityId +
                ", mytime=" + mytime +
                '}';
    }
}
