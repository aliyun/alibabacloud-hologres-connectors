package io.hologres.flink.ordergen;

import java.io.Serializable;

/**
 * City.
 */
public class City implements Serializable {
    private String nameZh;
    private String name;
    private String code;
    private String longtitude;
    private String latitude;

    public City(String nameZh, String name, String code, String longitude, String latitude) {
        this.nameZh = nameZh;
        this.name = name;
        this.code = code;
        this.longtitude = longitude;
        this.latitude = latitude;
    }

    public String getLongtitude() {
        return longtitude;
    }

    public String getLatitude() {
        return latitude;
    }
}
